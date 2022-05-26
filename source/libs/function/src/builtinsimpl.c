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
#include "querynodes.h"
#include "taggfunction.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdigest.h"
#include "thistogram.h"
#include "tpercentile.h"

#define HISTOGRAM_MAX_BINS_NUM   1000
#define MAVG_MAX_POINTS_NUM      1000
#define SAMPLE_MAX_POINTS_NUM    1000
#define TAIL_MAX_POINTS_NUM      100
#define TAIL_MAX_OFFSET          100

#define UNIQUE_MAX_RESULT_SIZE (1024*1024*10)

#define HLL_BUCKET_BITS 14 // The bits of the bucket
#define HLL_DATA_BITS (64-HLL_BUCKET_BITS)
#define HLL_BUCKETS (1<<HLL_BUCKET_BITS)
#define HLL_BUCKET_MASK (HLL_BUCKETS-1)
#define HLL_ALPHA_INF 0.721347520444481703680 // constant for 0.5/ln(2)


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
} SAvgRes;

typedef struct STuplePos {
 int32_t pageId;
 int32_t offset;
} STuplePos;

typedef struct STopBotResItem {
  SVariant  v;
  uint64_t  uid;  // it is a table uid, used to extract tag data during building of the final result for the tag data
  STuplePos tuplePos;  // tuple data of this chosen row
} STopBotResItem;

typedef struct STopBotRes {
  STopBotResItem* pItems;
} STopBotRes;

typedef struct SStddevRes {
  double  result;
  int64_t count;
  union {
    double  quadraticDSum;
    int64_t quadraticISum;
  };
  union {
    double  dsum;
    int64_t isum;
  };
} SStddevRes;

typedef struct SLeastSQRInfo {
  double matrix[2][3];
  double startVal;
  double stepVal;
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
  double result;
  int8_t algo;
  SHistogramInfo *pHisto;
  TDigest *pTDigest;
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

typedef struct SHistoFuncBin {
  double lower;
  double upper;
  union {
    int64_t count;
    double  percentage;
  };
} SHistoFuncBin;

typedef struct SHistoFuncInfo {
  int32_t numOfBins;
  int32_t totalCount;
  bool    normalized;
  SHistoFuncBin bins[];
} SHistoFuncInfo;

typedef enum {
  UNKNOWN_BIN = 0,
  USER_INPUT_BIN,
  LINEAR_BIN,
  LOG_BIN
} EHistoBinType;

typedef struct SHLLFuncInfo {
  uint64_t result;
  uint8_t buckets[HLL_BUCKETS];
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
  int32_t samples;
  int32_t totalPoints;
  int32_t numSampled;
  uint8_t colType;
  int16_t colBytes;
  char *data;
  int64_t *timestamp;
} SSampleInfo;

typedef struct STailItem {
  int64_t timestamp;
  bool    isNull;
  char    data[];
} STailItem;

typedef struct STailInfo {
  int32_t   numOfPoints;
  int32_t   numAdded;
  int32_t   offset;
  uint8_t   colType;
  int16_t   colBytes;
  STailItem **pItems;
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
  SHashObj  *pHash;
  char      pItems[];
} SUniqueInfo;

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
  //pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  colDataAppend(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  char*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type = pDestCtx->input.pData[0]->info.type;
  int32_t bytes = pDestCtx->input.pData[0]->info.bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  char*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (pSResInfo->numOfRes != 0 &&
        (pDResInfo->numOfRes == 0 || *(TSKEY*)(pDBuf + bytes) > *(TSKEY*)(pSBuf + bytes)) ) {
    memcpy(pDBuf, pSBuf, bytes);
    *(TSKEY*)(pDBuf + bytes) = *(TSKEY*)(pSBuf + bytes);
    pDResInfo->numOfRes = 1;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t dummyProcess(SqlFunctionCtx* UNUSED_PARAM(pCtx)) {
  return 0;
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

static FORCE_INLINE int32_t getNumofElem(SqlFunctionCtx* pCtx) {
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
  int32_t numOfElem = getNumofElem(pCtx);
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t type   = pInput->pData[0]->info.type;

  char* buf = GET_ROWCELL_INTERBUF(pResInfo);
  if (IS_NULL_TYPE(type)) {
    //select count(NULL) returns 0
    numOfElem = 1;
    *((int64_t*)buf) = 0;
  } else {
    *((int64_t*)buf) += numOfElem;
  }

  SET_VAL(pResInfo, numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t countInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = getNumofElem(pCtx);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t*)buf) -= numOfElem;

  SET_VAL(pResInfo, *((int64_t*)buf), 1);
  return TSDB_CODE_SUCCESS;
}

int32_t combineFunction(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  char* pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  char*                pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  *((int64_t*)pDBuf) += *((int64_t*)pSBuf);

  SET_VAL(pDResInfo, *((int64_t*)pDBuf), 1);
  return TSDB_CODE_SUCCESS;
}

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

int32_t sumFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg   = pInput->pColumnDataAgg[0];
  int32_t               type   = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (IS_NULL_TYPE(type)) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElem = 1;
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

  //check for overflow
  if (IS_FLOAT_TYPE(type) && (isinf(pSumRes->dsum) || isnan(pSumRes->dsum))) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
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
  SSumRes* pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type  = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSumRes* pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  
  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    pDBuf->isum += pSBuf->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->usum += pSBuf->usum;
  } else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
    pDBuf->dsum += pSBuf->dsum;
  }

  SET_VAL(pDResInfo, *((int64_t*)pDBuf), 1);
  return TSDB_CODE_SUCCESS;
}

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

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

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SAvgRes* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(type)) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElem = 1;
    goto _avg_over;
  }

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

_avg_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

#define LIST_AVG_N(sumT, T)                                                   \
  do {                                                                        \
      T* plist = (T*)pCol->pData;                                             \
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {   \
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {          \
          continue;                                                           \
        }                                                                     \
                                                                              \
        numOfElem += 1;                                                       \
        pAvgRes->count -= 1;                                                  \
        sumT -= plist[i];                                                     \
      }                                                                       \
  } while (0)

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
  SAvgRes*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAvgRes*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (IS_INTEGER_TYPE(type)) {
    pDBuf->sum.isum += pSBuf->sum.isum;
  } else {
    pDBuf->sum.dsum += pSBuf->sum.dsum;
  }
  pDBuf->count += pSBuf->count;

  return TSDB_CODE_SUCCESS;
}

int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;
  SAvgRes*              pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (IS_INTEGER_TYPE(type)) {
    pAvgRes->result = pAvgRes->sum.isum / ((double)pAvgRes->count);
  } else {
    if (isinf(pAvgRes->sum.dsum) || isnan(pAvgRes->sum.dsum)) {
      GET_RES_INFO(pCtx)->isNullRes = 1;
    }
    pAvgRes->result = pAvgRes->sum.dsum / ((double)pAvgRes->count);
  }

  return functionFinalize(pCtx, pBlock);
}

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  return FUNC_DATA_REQUIRED_STATIS_LOAD;
}

typedef struct SMinmaxResInfo {
  bool      assign;   // assign the first value or not
  int64_t   v;
  STuplePos tuplePos;
} SMinmaxResInfo;

bool minmaxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  SMinmaxResInfo* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  buf->assign = false;
  buf->tuplePos.pageId = -1;
  return true;
}

bool getMinmaxFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMinmaxResInfo);
  return true;
}

static void saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
static void copyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc) {
  int32_t numOfElems = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo *pBuf = GET_ROWCELL_INTERBUF(pResInfo);

  if (IS_NULL_TYPE(type)) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElems = 1;
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
      index = pInput->pColumnDataAgg[0]->minIndex;
    } else {
      tval = &pInput->pColumnDataAgg[0]->max;
      index = pInput->pColumnDataAgg[0]->maxIndex;
    }

    // the index is the original position, not the relative position
    TSKEY key = (pCtx->ptsList != NULL) ? pCtx->ptsList[index] : TSKEY_INITIAL_VAL;

    if (!pBuf->assign) {
      pBuf->v = *(int64_t*)tval;
      if (pCtx->subsidiaries.num > 0) {
        saveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
      }
    } else {
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        int64_t prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        int64_t val = GET_INT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            saveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }

      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        uint64_t prev = 0;
        GET_TYPED_DATA(prev, uint64_t, type, &pBuf->v);

        uint64_t val = GET_UINT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            saveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      } else if (type == TSDB_DATA_TYPE_DOUBLE) {
        double prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        double val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            saveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
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
          saveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
              copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
          saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      }

      numOfElems += 1;
    }
  } else if (type == TSDB_DATA_TYPE_FLOAT) {
    float* pData = (float*)pCol->pData;
    double* val = (double*)&pBuf->v;

    for (int32_t i = start; i < start + numOfRows; ++i) {
      if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      if (!pBuf->assign) {
        *val = pData[i];
        if (pCtx->subsidiaries.num > 0) {
          saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
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
            copyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      }

      numOfElems += 1;
    }
  }

_min_max_over:
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

static void setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos *pTuplePos, int32_t rowIndex);

int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  SMinmaxResInfo* pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value
  int32_t currentRow = pBlock->info.rows;

  if (pCol->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = *(double*) &pRes->v;
    colDataAppend(pCol, currentRow, (const char*)&v, pEntryInfo->isNullRes);
  } else {
    colDataAppend(pCol, currentRow, (const char*)&pRes->v, pEntryInfo->isNullRes);
  }

  setSelectivityValue(pCtx, pBlock, &pRes->tuplePos, currentRow);
  return pEntryInfo->numOfRes;
}

void setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos *pTuplePos, int32_t rowIndex) {
  int32_t pageId = pTuplePos->pageId;
  int32_t offset = pTuplePos->offset;
  if (pTuplePos->pageId != -1) {
    SFilePage* pPage = getBufPage(pCtx->pBuf, pageId);

    bool* nullList = (bool*)((char*)pPage + offset);
    char* pStart = (char*)(nullList + pCtx->pSrcBlock->info.numOfCols * sizeof(bool));

    // todo set the offset value to optimize the performance.
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

      SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
      int32_t      srcSlotId = pFuncParam->pCol->slotId;
      int32_t      dstSlotId = pc->pExpr->base.resSchema.slotId;

      int32_t ps = 0;
      for (int32_t k = 0; k < srcSlotId; ++k) {
        SColumnInfoData* pSrcCol = taosArrayGet(pCtx->pSrcBlock->pDataBlock, k);
        ps += pSrcCol->info.bytes;
      }

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      if (nullList[srcSlotId]) {
        colDataAppendNULL(pDstCol, rowIndex);
      } else {
        colDataAppend(pDstCol, rowIndex, (pStart + ps), false);
      }
    }
  }
}

int32_t minMaxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t isMinFunc) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SMinmaxResInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SMinmaxResInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  if (IS_FLOAT_TYPE(type)) {
    if (pSBuf->assign && 
        ( (((*(double*)&pDBuf->v) < (*(double*)&pSBuf->v)) ^ isMinFunc) || !pDBuf->assign ) ) {
      *(double*) &pDBuf->v = *(double*) &pSBuf->v;
    }
  } else {
    if ( pSBuf->assign && ( ((pDBuf->v < pSBuf->v) ^ isMinFunc) || !pDBuf->assign ) ) {
      pDBuf->v = pSBuf->v;
    }
  }
  SET_VAL(pDResInfo, *((int64_t*)pDBuf), 1);
  return TSDB_CODE_SUCCESS;
}

int32_t minCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 1);
}
int32_t maxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 0);
}

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
  int32_t               type = pInput->pData[0]->info.type;
  SStddevRes*           pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double                avg;
  if (IS_INTEGER_TYPE(type)) {
    avg = pStddevRes->isum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(pStddevRes->quadraticISum / ((double)pStddevRes->count) - avg * avg);
  } else {
    avg = pStddevRes->dsum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(pStddevRes->quadraticDSum / ((double)pStddevRes->count) - avg * avg);
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t stddevCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SStddevRes*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SStddevRes*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (IS_INTEGER_TYPE(type)) {
    pDBuf->isum += pSBuf->isum;
    pDBuf->quadraticISum += pSBuf->quadraticISum;
  } else {
    pDBuf->dsum += pSBuf->dsum;
    pDBuf->quadraticDSum += pSBuf->quadraticDSum;
  }
  pDBuf->count += pSBuf->count;
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

  pInfo->startVal = IS_FLOAT_TYPE(pCtx->param[1].param.nType) ? pCtx->param[1].param.d :
                                                                (double)pCtx->param[1].param.i;
  pInfo->stepVal = IS_FLOAT_TYPE(pCtx->param[1].param.nType) ? pCtx->param[2].param.d :
                                                                (double)pCtx->param[1].param.i;
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

        break;
      }
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
  SLeastSQRInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t        slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;

  if (0 == pInfo->num) {
    return 0;
  }

  double(*param)[3] = pInfo->matrix;

  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];

  param[0][0] -= param[1][0] * (param[0][1] / param[1][1]);
  param[0][2] -= param[1][2] * (param[0][1] / param[1][1]);
  param[0][1] = 0;
  param[1][2] -= param[0][2] * (param[1][0] / param[0][0]);
  param[1][0] = 0;
  param[0][2] /= param[0][0];

  param[1][2] /= param[1][1];

  char buf[64] = {0};
  size_t len = snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%.6lf, intercept:%.6lf}", param[0][2], param[1][2]);
  varDataSetLen(buf, len);

  colDataAppend(pCol, currentRow, buf, false);

  return pResInfo->numOfRes;
}

int32_t leastSQRInvertFunction(SqlFunctionCtx* pCtx) {
  //TODO
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
  int32_t              notNullElems = 0;
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
      notNullElems += 1;
      tMemBucketPut(pInfo->pMemBucket, data, 1);
    }

    SET_VAL(pResInfo, notNullElems, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SVariant* pVal = &pCtx->param[1].param;
  double    v = (pVal->nType == TSDB_DATA_TYPE_BIGINT) ? pVal->i : pVal->d;

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
  int32_t bytesHist   = (int32_t)(sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
  int32_t bytesDigest = (int32_t)(sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
  pEnv->calcMemSize = TMAX(bytesHist, bytesDigest);
  return true;
}

static int8_t getApercentileAlgo(char *algoStr) {
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
  pInfo->pHisto = (SHistogramInfo*) ((char*) pInfo + sizeof(SAPercentileInfo));
  pInfo->pHisto->elems = (SHistBin*) ((char*)pInfo->pHisto + sizeof(SHistogramInfo));
}

bool apercentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  if (pCtx->numOfParams == 2) {
    pInfo->algo = APERCT_ALGO_DEFAULT;
  } else if (pCtx->numOfParams == 3) {
    pInfo->algo = getApercentileAlgo(pCtx->param[2].param.pz);
    if (pInfo->algo == APERCT_ALGO_UNKNOWN) {
      return false;
    }
  }

  char *tmp = (char *)pInfo + sizeof(SAPercentileInfo);
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    pInfo->pTDigest = tdigestNewFrom(tmp, COMPRESSION);
  } else {
    buildHistogramInfo(pInfo);
    pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
  }

  return true;
}

int32_t apercentileFunction(SqlFunctionCtx* pCtx) {
  int32_t              notNullElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  //SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t start = pInput->startRowIndex;
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      notNullElems += 1;
      char* data = colDataGetData(pCol, i);

      double v = 0; // value
      int64_t w = 1; // weigth
      GET_TYPED_DATA(v, double, type, data);
      tdigestAdd(pInfo->pTDigest, v, w);
    }
  } else {
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      notNullElems += 1;
      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      tHistogramAdd(&pInfo->pHisto, v);
    }
  }

  SET_VAL(pResInfo, notNullElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SVariant* pVal    = &pCtx->param[1].param;
  double    percent = (pVal->nType == TSDB_DATA_TYPE_BIGINT) ? pVal->i : pVal->d;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*       pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    if (pInfo->pTDigest->size > 0) {
      pInfo->result = tdigestQuantile(pInfo->pTDigest, percent/100);
    } else {  // no need to free
      //setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return TSDB_CODE_SUCCESS;
    }
  } else {
    if (pInfo->pHisto->numOfElems > 0) {
      double ratio[] = {percent};
      double *res = tHistogramUniform(pInfo->pHisto, ratio, 1);
      pInfo->result = *res;
      //memcpy(pCtx->pOutput, res, sizeof(double));
      taosMemoryFree(res);
    } else {  // no need to free
      //setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return TSDB_CODE_SUCCESS;
    }
  }

  return functionFinalize(pCtx, pBlock);
}

bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes + sizeof(int64_t);
  return true;
}

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes;
  return true;
}

static FORCE_INLINE TSKEY getRowPTs(SColumnInfoData* pTsColInfo, int32_t rowIndex) {
  if (pTsColInfo == NULL) {
    return 0;
  }

  return *(TSKEY*)colDataGetData(pTsColInfo, rowIndex);
}

// This ordinary first function does not care if current scan is ascending order or descending order scan
// the OPTIMIZED version of first function will only handle the ascending order scan
int32_t firstFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t bytes = pInputCol->info.bytes;

  // All null data column, return directly.
  if (pInput->colDataAggIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows)) {
    ASSERT(pInputCol->hasNull == true);
    return 0;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataAggIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  if (blockDataOrder == TSDB_ORDER_ASC) {
    // filter according to current result firstly
    if (pResInfo->numOfRes > 0) {
      TSKEY ts = *(TSKEY*)(buf + bytes);
      if (ts < startKey) {
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

      if (pResInfo->numOfRes == 0 || *(TSKEY*)(buf + bytes) > cts) {
        memcpy(buf, data, bytes);
        *(TSKEY*)(buf + bytes) = cts;
        //        DO_UPDATE_TAG_COLUMNS(pCtx, ts);

        pResInfo->numOfRes = 1;
        break;
      }
    }
  } else {
    // in case of descending order time stamp serial, which usually happens as the results of the nest query,
    // all data needs to be check.
    if (pResInfo->numOfRes > 0) {
      TSKEY ts = *(TSKEY*)(buf + bytes);
      if (ts < endKey) {
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

      if (pResInfo->numOfRes == 0 || *(TSKEY*)(buf + bytes) > cts) {
        memcpy(buf, data, bytes);
        *(TSKEY*)(buf + bytes) = cts;
        //        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
        pResInfo->numOfRes = 1;
        break;
      }
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t lastFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t bytes = pInputCol->info.bytes;

  // All null data column, return directly.
  if (pInput->colDataAggIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows)) {
    ASSERT(pInputCol->hasNull == true);
    return 0;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataAggIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  if (blockDataOrder == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || *(TSKEY*)(buf + bytes) < cts) {
        memcpy(buf, data, bytes);
        *(TSKEY*)(buf + bytes) = cts;
        //        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
        pResInfo->numOfRes = 1;
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
      if (pResInfo->numOfRes == 0 || *(TSKEY*)(buf + bytes) < cts) {
        memcpy(buf, data, bytes);
        *(TSKEY*)(buf + bytes) = cts;
        pResInfo->numOfRes = 1;
        //        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
      }
      break;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t firstlastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  colDataAppend(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  char*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t type = pDestCtx->input.pData[0]->info.type;
  int32_t bytes = pDestCtx->input.pData[0]->info.bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  char*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (pSResInfo->numOfRes != 0 && 
        (pDResInfo->numOfRes == 0 || *(TSKEY*)(pDBuf + bytes) < *(TSKEY*)(pSBuf + bytes)) ) {
    memcpy(pDBuf, pSBuf, bytes);
    *(TSKEY*)(pDBuf + bytes) = *(TSKEY*)(pSBuf + bytes);
    pDResInfo->numOfRes = 1;
  }
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
  pDiffInfo->ignoreNegative = pCtx->param[1].param.i;  // TODO set correct param
  pDiffInfo->includeNull = false;
  pDiffInfo->firstOutput = false;
  return true;
}

static void doSetPrevVal(SDiffInfo* pDiffInfo, int32_t type, const char* pv) {
  switch(type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      pDiffInfo->prev.i64 = *(int8_t*) pv; break;
    case TSDB_DATA_TYPE_INT:
      pDiffInfo->prev.i64 = *(int32_t*) pv; break;
    case TSDB_DATA_TYPE_SMALLINT:
      pDiffInfo->prev.i64 = *(int16_t*) pv; break;
    case TSDB_DATA_TYPE_BIGINT:
      pDiffInfo->prev.i64 = *(int64_t*) pv; break;
    case TSDB_DATA_TYPE_FLOAT:
      pDiffInfo->prev.d64 = *(float *) pv; break;
    case TSDB_DATA_TYPE_DOUBLE:
      pDiffInfo->prev.d64 = *(double*) pv; break;
    default:
      ASSERT(0);
  }
}

static void doHandleDiff(SDiffInfo* pDiffInfo, int32_t type, const char* pv, SColumnInfoData* pOutput, int32_t pos, int32_t order) {
    int32_t factor = (order == TSDB_ORDER_ASC)? 1:-1;
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)pv;
      int32_t delta = factor*(v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt32(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t*)pv;
      int8_t delta = factor*(v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt8(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)pv;
      int16_t delta = factor*(v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt16(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)pv;
      int64_t delta = factor*(v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float*)pv;
      float delta = factor*(v - pDiffInfo->prev.d64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendFloat(pOutput, pos, &delta);
      }
      pDiffInfo->prev.d64 = v;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)pv;
      double delta = factor*(v - pDiffInfo->prev.d64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
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
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;

  int32_t numOfElems = 0;
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;
  int32_t startOffset = pCtx->offset;

  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  if (pCtx->order == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
      int32_t pos = startOffset + numOfElems;

      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        if (pDiffInfo->includeNull) {
          colDataSetNull_f(pOutput->nullbitmap, pos);
          if (tsList != NULL) {
            colDataAppendInt64(pTsOutput, pos, &tsList[i]);
          }

          numOfElems += 1;
        }
        continue;
      }

      char* pv = colDataGetData(pInputCol, i);

      if (pDiffInfo->hasPrev) {
        doHandleDiff(pDiffInfo, pInputCol->info.type, pv, pOutput, pos, pCtx->order);
        if (pTsOutput != NULL) {
          colDataAppendInt64(pTsOutput, pos, &tsList[i]);
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
          if (tsList != NULL) {
            colDataAppendInt64(pTsOutput, pos, &tsList[i]);
          }

          numOfElems += 1;
        }
        continue;
      }

      char* pv = colDataGetData(pInputCol, i);

      // there is a row of previous data block to be handled in the first place.
      if (pDiffInfo->hasPrev) {
        doHandleDiff(pDiffInfo, pInputCol->info.type, pv, pOutput, pos, pCtx->order);
        if (pTsOutput != NULL) {
          colDataAppendInt64(pTsOutput, pos, &pDiffInfo->prevTs);
        }

        numOfElems++;
      } else {
        doSetPrevVal(pDiffInfo, pInputCol->info.type, pv);
      }

      pDiffInfo->hasPrev = true;
      if (pTsOutput != NULL) {
        pDiffInfo->prevTs = tsList[i];
      }
    }
  }

  // initial value is not set yet
  return numOfElems;
}

bool getTopBotFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SValueNode* pkNode = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  pEnv->calcMemSize = sizeof(STopBotRes) + pkNode->datum.i * sizeof(STopBotResItem);
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

int32_t topFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char* data = colDataGetData(pCol, i);
    doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, type, pInput->uid, pResInfo, true);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t bottomFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char* data = colDataGetData(pCol, i);
    doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, type, pInput->uid, pResInfo, false);
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
  int32_t     maxSize = pCtx->param[1].param.i;

  SVariant val = {0};
  taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);

  STopBotResItem* pItems = pRes->pItems;
  assert(pItems != NULL);

  // not full yet
  if (pEntryInfo->numOfRes < maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = val;
    pItem->uid = uid;

    // save the data of this tuple
    saveTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);

    // allocate the buffer and keep the data of this row into the new allocated buffer
    pEntryInfo->numOfRes++;
    taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type, topBotResComparFn,
                 !isTopQuery);
  } else {  // replace the minimum value in the result
    if ((isTopQuery && (
        (IS_SIGNED_NUMERIC_TYPE(type) && val.i > pItems[0].v.i) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u > pItems[0].v.u) ||
        (IS_FLOAT_TYPE(type) && val.d > pItems[0].v.d)))
        || (!isTopQuery && (
        (IS_SIGNED_NUMERIC_TYPE(type) && val.i < pItems[0].v.i) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u < pItems[0].v.u) ||
        (IS_FLOAT_TYPE(type) && val.d < pItems[0].v.d))
        )) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = val;
      pItem->uid = uid;

      // save the data of this tuple by over writing the old data
      copyTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
      taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, !isTopQuery);
    }
  }
}

void saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  SFilePage* pPage = NULL;

  int32_t completeRowSize = pSrcBlock->info.rowSize + pSrcBlock->info.numOfCols * sizeof(bool);

  if (pCtx->curBufPage == -1) {
    pPage = getNewBufPage(pCtx->pBuf, 0, &pCtx->curBufPage);
    pPage->num = sizeof(SFilePage);
  } else {
    pPage = getBufPage(pCtx->pBuf, pCtx->curBufPage);
    if (pPage->num + completeRowSize > getBufPageSize(pCtx->pBuf)) {
      pPage = getNewBufPage(pCtx->pBuf, 0, &pCtx->curBufPage);
      pPage->num = sizeof(SFilePage);
    }
  }

  pPos->pageId = pCtx->curBufPage;

  // keep the current row data, extract method
  int32_t offset = 0;
  bool*   nullList = (bool*)((char*)pPage + pPage->num);
  char*   pStart = (char*)(nullList + sizeof(bool) * pSrcBlock->info.numOfCols);
  for (int32_t i = 0; i < pSrcBlock->info.numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, i);
    bool             isNull = colDataIsNull_s(pCol, rowIndex);
    if (isNull) {
      nullList[i] = true;
      offset += pCol->info.bytes;
      continue;
    }

    char* p = colDataGetData(pCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart + offset, p, varDataTLen(p));
    } else {
      memcpy(pStart + offset, p, pCol->info.bytes);
    }

    offset += pCol->info.bytes;
  }

  pPos->offset = pPage->num;
  pPage->num += completeRowSize;

  setBufPageDirty(pPage, true);
  releaseBufPage(pCtx->pBuf, pPage);
}

void copyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  SFilePage* pPage = getBufPage(pCtx->pBuf, pPos->pageId);

  bool* nullList = (bool*)((char*)pPage + pPos->offset);
  char* pStart = (char*)(nullList + pSrcBlock->info.numOfCols * sizeof(bool));

  int32_t offset = 0;
  for (int32_t i = 0; i < pSrcBlock->info.numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, i);
    if ((nullList[i] = colDataIsNull_s(pCol, rowIndex)) == true) {
      continue;
    }

    char* p = colDataGetData(pCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart + offset, p, varDataTLen(p));
    } else {
      memcpy(pStart + offset, p, pCol->info.bytes);
    }

    offset += pCol->info.bytes;
  }

  setBufPageDirty(pPage, true);
  releaseBufPage(pCtx->pBuf, pPage);
}

int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = GET_ROWCELL_INTERBUF(pEntryInfo);
  pEntryInfo->complete = true;

  int32_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STopBotResItem* pItem = &pRes->pItems[i];
    if (type == TSDB_DATA_TYPE_FLOAT) {
      float v = pItem->v.d;
      colDataAppend(pCol, currentRow, (const char*)&v, false);
    } else {
      colDataAppend(pCol, currentRow, (const char*)&pItem->v.i, false);
    }

    setSelectivityValue(pCtx, pBlock, &pRes->pItems[i].tuplePos, currentRow);
    currentRow += 1;
  }

  return pEntryInfo->numOfRes;
}

bool getSpreadFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSpreadInfo);
  return true;
}

bool spreadFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->min, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->max, -DBL_MAX);
  pInfo->hasResult = false;
  return true;
}

int32_t spreadFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];
  int32_t type = pInput->pData[0]->info.type;

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

    int32_t start     = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    // check the valid data one by one
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      char *data = colDataGetData(pCol, i);

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

int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (pInfo->hasResult == true) {
    SET_DOUBLE_VAL(&pInfo->result, pInfo->max - pInfo->min);
  }
  return functionFinalize(pCtx, pBlock);
}

bool getElapsedFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SElapsedInfo);
  return true;
}

bool elapsedFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->result = 0;
  pInfo->min = MAX_TS_KEY;
  pInfo->max = 0;

  if (pCtx->numOfParams == 2) {
    pInfo->timeUnit = pCtx->param[1].param.i;
  } else {
    pInfo->timeUnit = 1;
  }

  return true;
}

int32_t elapsedFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  numOfElems = pInput->numOfRows; //since this is the primary timestamp, no need to exclude NULL values
  if (numOfElems == 0) {
    goto _elapsed_over;
  }

  if (pInput->colDataAggIsSet) {
    if (pInfo->min == MAX_TS_KEY) {
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

    int32_t start     = pInput->startRowIndex;
    TSKEY* ptsList = (int64_t*)colDataGetData(pCol, start);
    if (pCtx->order == TSDB_ORDER_DESC) {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->max = (pInfo->max < ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->max;
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
        pInfo->max = ptsList[start + pInput->numOfRows - 1];
      }
    }
  }

_elapsed_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double result = (double)pInfo->max - (double)pInfo->min;
  result = (result >= 0) ? result : -result;
  pInfo->result = result / pInfo->timeUnit;
  return functionFinalize(pCtx, pBlock);
}

bool getHistogramFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHistoFuncInfo) + HISTOGRAM_MAX_BINS_NUM * sizeof(SHistoFuncBin);
  return true;
}

static int8_t getHistogramBinType(char *binTypeStr) {
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

static bool getHistogramBinDesc(SHistoFuncInfo *pInfo, char *binDescStr, int8_t binType, bool normalized) {
  cJSON*  binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double* intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      return false;
    }

    cJSON* start    = cJSON_GetObjectItem(binDesc, "start");
    cJSON* factor   = cJSON_GetObjectItem(binDesc, "factor");
    cJSON* width    = cJSON_GetObjectItem(binDesc, "width");
    cJSON* count    = cJSON_GetObjectItem(binDesc, "count");
    cJSON* infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      return false;
    }

    if (count->valueint <= 0 || count->valueint > 1000) { // limit count to 1000
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

  pInfo->numOfBins  = numOfBins - 1;
  pInfo->normalized = normalized;
  for (int32_t i = 0; i < pInfo->numOfBins; ++i) {
    pInfo->bins[i].lower = intervals[i] < intervals[i + 1] ? intervals[i] : intervals[i + 1];
    pInfo->bins[i].upper = intervals[i + 1] > intervals[i] ? intervals[i + 1] : intervals[i];
    pInfo->bins[i].count = 0;
  }

  taosMemoryFree(intervals);
  return true;
}

bool histogramFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo *pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SHistoFuncInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->numOfBins = 0;
  pInfo->totalCount = 0;
  pInfo->normalized = 0;

  int8_t binType = getHistogramBinType(varDataVal(pCtx->param[1].param.pz));
  if (binType == UNKNOWN_BIN) {
    return false;
  }
  char* binDesc = varDataVal(pCtx->param[2].param.pz);
  int64_t normalized = pCtx->param[3].param.i;
  if (normalized != 0 && normalized != 1) {
    return false;
  }
  if (!getHistogramBinDesc(pInfo, binDesc, binType, (bool)normalized)) {
    return false;
  }

  return true;
}

int32_t histogramFunction(SqlFunctionCtx *pCtx) {
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

    char* data = colDataGetData(pCol, i);
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

  SET_VAL(GET_RES_INFO(pCtx), numOfElems, pInfo->numOfBins);
  return TSDB_CODE_SUCCESS;
}

int32_t histogramFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t        slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;

  if (pInfo->normalized) {
    for (int32_t k = 0; k < pResInfo->numOfRes; ++k) {
      if(pInfo->totalCount != 0) {
        pInfo->bins[k].percentage = pInfo->bins[k].count / (double)pInfo->totalCount;
      } else {
        pInfo->bins[k].percentage = 0;
      }
    }
  }

  for (int32_t i = 0; i < pResInfo->numOfRes; ++i) {
    int32_t len;
    char buf[512] = {0};
    if (!pInfo->normalized) {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%"PRId64"}",
                   pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].count);
    } else {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}",
                   pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].percentage);
    }
    varDataSetLen(buf, len);
    colDataAppend(pCol, currentRow, buf, false);
    currentRow++;
  }

  return pResInfo->numOfRes;
}

bool getHLLFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHLLInfo);
  return true;
}

static uint8_t hllCountNum(void* data, int32_t bytes, int32_t *buk) {
  uint64_t hash = MurmurHash3_64(data, bytes);
  int32_t index = hash & HLL_BUCKET_MASK;
  hash >>= HLL_BUCKET_BITS;
  hash |= ((uint64_t)1 << HLL_DATA_BITS);
  uint64_t bit = 1;
  uint8_t count = 1;
  while((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *buk = index;
  return count;
}

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


int32_t hllFunction(SqlFunctionCtx *pCtx) {
  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type  = pCol->info.type;
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

int32_t hllFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  pInfo->result = hllCountCnt(pInfo->buckets);

  return functionFinalize(pCtx, pBlock);
}

bool getStateFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStateInfo);
  return true;
}

static int8_t getStateOpType(char *opStr) {
  int8_t opType;
  if (strcasecmp(opStr, "LT") == 0) {
    opType = STATE_OPER_LT;
  } else if (strcasecmp(opStr, "GT") == 0) {
    opType = STATE_OPER_GT;
  } else if (strcasecmp(opStr, "LE") == 0) {
    opType = STATE_OPER_LE;
  } else if (strcasecmp(opStr, "GE") == 0) {
    opType = STATE_OPER_GE;
  } else if (strcasecmp(opStr, "NE") == 0) {
    opType = STATE_OPER_NE;
  } else if (strcasecmp(opStr, "EQ") == 0) {
    opType = STATE_OPER_EQ;
  } else {
    opType = STATE_OPER_INVALID;
  }

  return opType;
}

#define GET_STATE_VAL(param) \
  ((param.nType == TSDB_DATA_TYPE_BIGINT) ? (param.i) : (param.d))

#define STATE_COMP(_op, _lval, _param)  \
  STATE_COMP_IMPL(_op, _lval, GET_STATE_VAL(_param))

#define STATE_COMP_IMPL(_op, _lval, _rval)  \
  do {                                      \
    switch(_op) {                           \
      case STATE_OPER_LT:                   \
        return ((_lval) < (_rval));         \
        break;                              \
      case STATE_OPER_GT:                   \
        return ((_lval) > (_rval));         \
        break;                              \
      case STATE_OPER_LE:                   \
        return ((_lval) <= (_rval));        \
        break;                              \
      case STATE_OPER_GE:                   \
        return ((_lval) >= (_rval));        \
        break;                              \
      case STATE_OPER_NE:                   \
        return ((_lval) != (_rval));        \
        break;                              \
      case STATE_OPER_EQ:                   \
        return ((_lval) == (_rval));        \
        break;                              \
      default:                              \
        break;                              \
    }                                       \
  } while (0)

static bool checkStateOp(int8_t op, SColumnInfoData* pCol, int32_t index, SVariant param) {
  char* data = colDataGetData(pCol, index);
  switch(pCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t v = *(uint8_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t v = *(uint16_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t v = *(uint32_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t v = *(uint64_t *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float *)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double *)data;
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

  int32_t numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return 0;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    numOfElems++;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataAppendNULL(pOutput, i);
      continue;
    }

    bool ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);
    int64_t output = -1;
    if (ret) {
      output = ++pInfo->count;
    } else {
      pInfo->count = 0;
    }
    colDataAppend(pOutput, i, (char *)&output, false);
  }

  return numOfElems;
}

int32_t stateDurationFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  //TODO: process timeUnit for different db precisions
  int32_t timeUnit = 1000;
  if (pCtx->numOfParams == 5) { //TODO: param number incorrect
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
      continue;
    }

    bool ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);
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
    colDataAppend(pOutput, i, (char *)&output, false);
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
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      //colDataAppendNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t v;
      GET_TYPED_DATA(v, int64_t, type, data);
      pSumRes->isum += v;
      colDataAppend(pOutput, pos, (char *)&pSumRes->isum, false);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t v;
      GET_TYPED_DATA(v, uint64_t, type, data);
      pSumRes->usum += v;
      colDataAppend(pOutput, pos, (char *)&pSumRes->usum, false);
    } else if (IS_FLOAT_TYPE(type)) {
      double v;
      GET_TYPED_DATA(v, double, type, data);
      pSumRes->dsum += v;
      colDataAppend(pOutput, pos, (char *)&pSumRes->dsum, false);
    }

    //TODO: remove this after pTsOutput is handled
    if (pTsOutput != NULL) {
      colDataAppendInt64(pTsOutput, pos, &tsList[i]);
    }

    numOfElems++;
  }

  return numOfElems;
}

bool getMavgFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMavgInfo) + MAVG_MAX_POINTS_NUM * sizeof(double);
  return true;
}

bool mavgFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo *pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SMavgInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
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
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      //colDataAppendNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    if (!pInfo->pointsMeet && (pInfo->pos < pInfo->numOfPoints - 1)) {
      pInfo->points[pInfo->pos] = v;
      pInfo->sum += v;
    } else {
      if (!pInfo->pointsMeet && (pInfo->pos == pInfo->numOfPoints - 1)) {
        pInfo->sum +=v;
        pInfo->pointsMeet = true;
      } else {
        pInfo->sum = pInfo->sum + v - pInfo->points[pInfo->pos];
      }

      pInfo->points[pInfo->pos] = v;
      double result = pInfo->sum / pInfo->numOfPoints;
      colDataAppend(pOutput, pos, (char *)&result, false);

      //TODO: remove this after pTsOutput is handled
      if (pTsOutput != NULL) {
        colDataAppendInt64(pTsOutput, pos, &tsList[i]);
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
  SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t numOfSamples = pVal->datum.i;
  pEnv->calcMemSize = sizeof(SSampleInfo) + numOfSamples * (pCol->node.resType.bytes + sizeof(int64_t));
  return true;
}

bool sampleFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo *pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  taosSeedRand(taosSafeRand());

  SSampleInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->samples = pCtx->param[1].param.i;
  pInfo->totalPoints = 0;
  pInfo->numSampled = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->samples < 1 || pInfo->samples > SAMPLE_MAX_POINTS_NUM) {
    return false;
  }
  pInfo->data = (char *)pInfo + sizeof(SSampleInfo);
  pInfo->timestamp = (int64_t *)((char *)pInfo + sizeof(SSampleInfo) + pInfo->samples * pInfo->colBytes);

  return true;
}

static void sampleAssignResult(SSampleInfo* pInfo, char *data, TSKEY ts, int32_t index) {
  assignVal(pInfo->data + index * pInfo->colBytes, data, pInfo->colBytes, pInfo->colType);
  *(pInfo->timestamp + index) = ts;
}

static void doReservoirSample(SSampleInfo* pInfo, char *data, TSKEY ts, int32_t index) {
  pInfo->totalPoints++;
  if (pInfo->numSampled < pInfo->samples) {
    sampleAssignResult(pInfo, data, ts, pInfo->numSampled);
    pInfo->numSampled++;
  } else {
    int32_t j = taosRand() % (pInfo->totalPoints);
    if (j < pInfo->samples) {
      sampleAssignResult(pInfo, data, ts, j);
    }
  }
}

int32_t sampleFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (colDataIsNull_s(pInputCol, i)) {
      //colDataAppendNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    doReservoirSample(pInfo, data, tsList[i], i);
  }

  for (int32_t i = 0; i < pInfo->numSampled; ++i) {
    int32_t pos = startOffset + i;
    colDataAppend(pOutput, pos, pInfo->data + i * pInfo->colBytes, false);
    //TODO: handle ts output
  }

  return pInfo->numSampled;
}

//int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
//  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
//  SSampleInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
//  int32_t        slotId = pCtx->pExpr->base.resSchema.slotId;
//  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
//
//  //int32_t currentRow = pBlock->info.rows;
//  pResInfo->numOfRes = pInfo->numSampled;
//
//  for (int32_t i = 0; i < pInfo->numSampled; ++i) {
//    colDataAppend(pCol, i, pInfo->data + i * pInfo->colBytes, false);
//    //TODO: handle ts output
//  }
//
//  return pResInfo->numOfRes;
//}


bool getTailFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t numOfPoints = pVal->datum.i;
  pEnv->calcMemSize = sizeof(STailInfo) + numOfPoints * (POINTER_BYTES + sizeof(STailItem) + pCol->node.resType.bytes);
  return true;
}

bool tailFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo *pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  STailInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
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

  pInfo->pItems = (STailItem **)((char *)pInfo + sizeof(STailInfo));
  char *pItem = (char *)pInfo->pItems + pInfo->numOfPoints * POINTER_BYTES;

  size_t unitSize = sizeof(STailItem) + pInfo->colBytes;
  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    pInfo->pItems[i] = (STailItem *)(pItem + i * unitSize);
    pInfo->pItems[i]->isNull = false;
  }

  return true;
}

static void tailAssignResult(STailItem* pItem, char *data, int32_t colBytes, TSKEY ts, bool isNull) {
  pItem->timestamp = ts;
  if (isNull) {
    pItem->isNull = true;
  } else {
    memcpy(pItem->data, data, colBytes);
  }
}

static int32_t tailCompFn(const void *p1, const void *p2, const void *param) {
  STailItem *d1 = *(STailItem **)p1;
  STailItem *d2 = *(STailItem **)p2;
  return compareInt64Val(&d1->timestamp, &d2->timestamp);
}

static void doTailAdd(STailInfo* pInfo, char *data, TSKEY ts, bool isNull) {
  STailItem **pList = pInfo->pItems;
  if (pInfo->numAdded < pInfo->numOfPoints) {
    tailAssignResult(pList[pInfo->numAdded], data, pInfo->colBytes, ts, isNull);
    taosheapsort((void *)pList, sizeof(STailItem **), pInfo->numAdded + 1, NULL, tailCompFn, 0);
    pInfo->numAdded++;
  } else if (pList[0]->timestamp < ts) {
    tailAssignResult(pList[0], data, pInfo->colBytes, ts, isNull);
    taosheapadjust((void *)pList, sizeof(STailItem **), 0, pInfo->numOfPoints - 1, NULL, tailCompFn, NULL, 0);
  }
}

int32_t tailFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STailInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
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
    int32_t pos = startOffset + i;
    STailItem *pItem = pInfo->pItems[i];
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
  STailInfo*                pInfo = GET_ROWCELL_INTERBUF(pEntryInfo);
  pEntryInfo->complete = true;

  int32_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STailItem *pItem = pInfo->pItems[i];
    colDataAppend(pCol, currentRow, pItem->data, false);

    //setSelectivityValue(pCtx, pBlock, &pInfo->pItems[i].tuplePos, currentRow);
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

static void doUniqueAdd(SUniqueInfo* pInfo, char *data, TSKEY ts, bool isNull) {
  int32_t hashKeyBytes = IS_VAR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;

  SUniqueItem *pHashItem = taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t size = sizeof(SUniqueItem) + pInfo->colBytes;
    SUniqueItem *pItem = (SUniqueItem *)(pInfo->pItems + pInfo->numOfPoints * size);
    pItem->timestamp = ts;
    memcpy(pItem->data, data, pInfo->colBytes);

    taosHashPut(pInfo->pHash, data, hashKeyBytes, (char *)pItem, sizeof(SUniqueItem*));
    pInfo->numOfPoints++;
  } else if (pHashItem->timestamp > ts) {
    pHashItem->timestamp = ts;
  }

}

int32_t uniqueFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SUniqueInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

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
    SUniqueItem *pItem = (SUniqueItem *)(pInfo->pItems + i * (sizeof(SUniqueItem) + pInfo->colBytes));
    colDataAppend(pOutput, i, pItem->data, false);
    if (pTsOutput != NULL) {
      colDataAppendInt64(pTsOutput, i, &pItem->timestamp);
    }
  }

  return pInfo->numOfPoints;
}

int32_t uniqueFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SUniqueInfo*    pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t        slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  for (int32_t i = 0; i < pResInfo->numOfRes; ++i) {
    SUniqueItem *pItem = (SUniqueItem *)(pInfo->pItems + i * (sizeof(SUniqueItem) + pInfo->colBytes));
    colDataAppend(pCol, i, pItem->data, false);
    //TODO: handle ts output
  }

  return pResInfo->numOfRes;
}

