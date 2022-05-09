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
#include "tdatablock.h"
#include "tpercentile.h"

#define HISTOGRAM_MAX_BINS_NUM   100

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

typedef struct STopBotResItem {
  SVariant v;
  uint64_t uid;  // it is a table uid, used to extract tag data during building of the final result for the tag data
  struct {
    int32_t pageId;
    int32_t offset;
  } tuplePos;  // tuple data of this chosen row
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

typedef struct SPercentileInfo {
  double      result;
  tMemBucket* pMemBucket;
  int32_t     stage;
  double      minval;
  double      maxval;
  int64_t     numOfElems;
} SPercentileInfo;

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
  bool    normalized;
  SHistoFuncBin bins[];
} SHistoFuncInfo;

typedef enum {
  UNKNOWN_BIN = 0,
  USER_INPUT_BIN,
  LINEAR_BIN,
  LOG_BIN
} EHistoBinType;


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
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;
  /*cleanupResultRowEntry(pResInfo);*/

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  colDataAppend(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
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
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t*)buf) += numOfElem;

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
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

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

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

bool getAvgFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(double);
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

int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;
  SAvgRes*              pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (IS_INTEGER_TYPE(type)) {
    pAvgRes->result = pAvgRes->sum.isum / ((double)pAvgRes->count);
  } else {
    pAvgRes->result = pAvgRes->sum.dsum / ((double)pAvgRes->count);
  }

  return functionFinalize(pCtx, pBlock);
}

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  return FUNC_DATA_REQUIRED_STATIS_LOAD;
}

bool maxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  char* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  switch (pCtx->resDataInfo.type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t*)buf) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t*)buf) = 0;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float*)buf) = -FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double*)buf), -DBL_MAX);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t*)buf) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t*)buf) = 0;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t*)buf) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t*)buf) = 0;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t*)buf) = INT8_MIN;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *((uint8_t*)buf) = 0;
      break;
    case TSDB_DATA_TYPE_BOOL:
      *((int8_t*)buf) = 0;
      break;
    default:
      assert(0);
  }
  return true;
}

bool minFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  char* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  switch (pCtx->resDataInfo.type) {
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t*)buf) = INT8_MAX;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t*)buf = UINT8_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t*)buf) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t*)buf) = UINT16_MAX;
      break;
    case TSDB_DATA_TYPE_INT:
      *((int32_t*)buf) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t*)buf) = UINT32_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t*)buf) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t*)buf) = UINT64_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float*)buf) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double*)buf), DBL_MAX);
      break;
    case TSDB_DATA_TYPE_BOOL:
      *((int8_t*)buf) = 1;
      break;
    default:
      assert(0);
  }

  return true;
}

bool getMinmaxFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

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

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc) {
  int32_t numOfElems = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);

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

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t prev = 0;
      GET_TYPED_DATA(prev, int64_t, type, buf);

      int64_t val = GET_INT64_VAL(tval);
      if ((prev < val) ^ isMinFunc) {
        *(int64_t*)buf = val;
        for (int32_t i = 0; i < (pCtx)->subsidiaries.num; ++i) {
          SqlFunctionCtx* __ctx = pCtx->subsidiaries.pCtx[i];
          if (__ctx->functionId == FUNCTION_TS_DUMMY) {  // TODO refactor
            __ctx->tag.i = key;
            __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
          }

          __ctx->fpSet.process(__ctx);
        }
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t prev = 0;
      GET_TYPED_DATA(prev, uint64_t, type, buf);

      uint64_t val = GET_UINT64_VAL(tval);
      if ((prev < val) ^ isMinFunc) {
        *(uint64_t*)buf = val;
        for (int32_t i = 0; i < (pCtx)->subsidiaries.num; ++i) {
          SqlFunctionCtx* __ctx = pCtx->subsidiaries.pCtx[i];
          if (__ctx->functionId == FUNCTION_TS_DUMMY) {  // TODO refactor
            __ctx->tag.i = key;
            __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
          }

          __ctx->fpSet.process(__ctx);
        }
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = GET_DOUBLE_VAL(tval);
      UPDATE_DATA(pCtx, *(double*)buf, val, numOfElems, isMinFunc, key);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      double val = GET_DOUBLE_VAL(tval);
      UPDATE_DATA(pCtx, *(float*)buf, val, numOfElems, isMinFunc, key);
    }

    return numOfElems;
  }

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
      LOOPCHECK_N(*(int8_t*)buf, pCol, pCtx, int8_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      LOOPCHECK_N(*(int16_t*)buf, pCol, pCtx, int16_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_INT) {
      int32_t* pData = (int32_t*)pCol->pData;
      int32_t* val = (int32_t*)buf;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if ((*val < pData[i]) ^ isMinFunc) {
          *val = pData[i];
          TSKEY ts = (pCtx->ptsList != NULL) ? GET_TS_DATA(pCtx, i) : 0;
          DO_UPDATE_SUBSID_RES(pCtx, ts);
        }

        numOfElems += 1;
      }

#if defined(_DEBUG_VIEW)
      qDebug("max value updated:%d", *retVal);
#endif
    } else if (type == TSDB_DATA_TYPE_BIGINT) {
      LOOPCHECK_N(*(int64_t*)buf, pCol, pCtx, int64_t, numOfRows, start, isMinFunc, numOfElems);
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    if (type == TSDB_DATA_TYPE_UTINYINT) {
      LOOPCHECK_N(*(uint8_t*)buf, pCol, pCtx, uint8_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_USMALLINT) {
      LOOPCHECK_N(*(uint16_t*)buf, pCol, pCtx, uint16_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_UINT) {
      LOOPCHECK_N(*(uint32_t*)buf, pCol, pCtx, uint32_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_UBIGINT) {
      LOOPCHECK_N(*(uint64_t*)buf, pCol, pCtx, uint64_t, numOfRows, start, isMinFunc, numOfElems);
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    LOOPCHECK_N(*(double*)buf, pCol, pCtx, double, numOfRows, start, isMinFunc, numOfElems);
  } else if (type == TSDB_DATA_TYPE_FLOAT) {
    LOOPCHECK_N(*(float*)buf, pCol, pCtx, float, numOfRows, start, isMinFunc, numOfElems);
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
  if (pCtx->currentStage == REPEAT_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      return 0;
    } else {
      pInfo->pMemBucket = tMemBucketCreate(pCtx->inputBytes, pCtx->inputType, pInfo->minval, pInfo->maxval);
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

    return 0;
  }

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
  return TSDB_CODE_SUCCESS;
}

int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SVariant* pVal = &pCtx->param[1].param;
  double    v = pVal->nType == TSDB_DATA_TYPE_INT ? pVal->i : pVal->d;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo*     ppInfo = (SPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  tMemBucket* pMemBucket = ppInfo->pMemBucket;
  if (pMemBucket != NULL && pMemBucket->total > 0) {  // check for null
    SET_DOUBLE_VAL(&ppInfo->result, getPercentile(pMemBucket, v));
  }

  tMemBucketDestroy(pMemBucket);
  return functionFinalize(pCtx, pBlock);
}

bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes + sizeof(int64_t);
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
                            uint64_t uid, SResultRowEntryInfo* pEntryInfo);

static void saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STopBotResItem* pItem);
static void copyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STopBotResItem* pItem);

int32_t topFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  //  if ((void *)pRes->res[0] != (void *)((char *)pRes + sizeof(STopBotRes) + POINTER_BYTES * pCtx->param[0].i)) {
  //    buildTopBotStruct(pRes, pCtx);
  //  }

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  for (int32_t i = start; i < numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }
    numOfElems++;

    char* data = colDataGetData(pCol, i);
    doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, type, pInput->uid, pResInfo);
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
                     uint64_t uid, SResultRowEntryInfo* pEntryInfo) {
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
    saveTupleData(pCtx, rowIndex, pSrcBlock, pItem);

    // allocate the buffer and keep the data of this row into the new allocated buffer
    pEntryInfo->numOfRes++;
    taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type, topBotResComparFn,
                 false);
  } else {  // replace the minimum value in the result
    if ((IS_SIGNED_NUMERIC_TYPE(type) && val.i > pItems[0].v.i) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u > pItems[0].v.u) || (IS_FLOAT_TYPE(type) && val.d > pItems[0].v.d)) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = val;
      pItem->uid = uid;

      // save the data of this tuple by over writing the old data
      copyTupleData(pCtx, rowIndex, pSrcBlock, pItem);

      taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, false);
    }
  }
}

void saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STopBotResItem* pItem) {
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

  pItem->tuplePos.pageId = pCtx->curBufPage;

  // keep the current row data, extract method
  int32_t offset = 0;
  bool*   nullList = (bool*)((char*)pPage + pPage->num);
  char*   pStart = (char*)(nullList + sizeof(bool) * pSrcBlock->info.numOfCols);
  for (int32_t i = 0; i < pSrcBlock->info.numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, i);
    bool             isNull = colDataIsNull_s(pCol, rowIndex);
    if (isNull) {
      nullList[i] = true;
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

  pItem->tuplePos.offset = pPage->num;
  pPage->num += completeRowSize;

  setBufPageDirty(pPage, true);
  releaseBufPage(pCtx->pBuf, pPage);
}

void copyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STopBotResItem* pItem) {
  SFilePage* pPage = getBufPage(pCtx->pBuf, pItem->tuplePos.pageId);

  bool* nullList = (bool*)((char*)pPage + pItem->tuplePos.offset);
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

  int32_t          type = pCtx->input.pData[0]->info.type;
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
        STopBotResItem* pItem = &pRes->pItems[i];
        colDataAppendInt32(pCol, currentRow, (int32_t*)&pItem->v.i);

        int32_t pageId = pItem->tuplePos.pageId;
        int32_t offset = pItem->tuplePos.offset;
        if (pItem->tuplePos.pageId != -1) {
          SFilePage* pPage = getBufPage(pCtx->pBuf, pageId);

          bool* nullList = (bool*)((char*)pPage + offset);
          char* pStart = (char*)(nullList + pCtx->pSrcBlock->info.numOfCols * sizeof(bool));

          // todo set the offset value to optimize the performance.
          for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
            SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

            SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
            int32_t      srcSlotId = pFuncParam->pCol->slotId;
            int32_t      dstSlotId = pCtx->pExpr->base.resSchema.slotId;

            int32_t ps = 0;
            for (int32_t k = 0; k < srcSlotId; ++k) {
              SColumnInfoData* pSrcCol = taosArrayGet(pCtx->pSrcBlock->pDataBlock, k);
              ps += pSrcCol->info.bytes;
            }

            SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
            if (nullList[srcSlotId]) {
              colDataAppendNULL(pDstCol, currentRow);
            } else {
              colDataAppend(pDstCol, currentRow, (pStart + ps), false);
            }
          }
        }

        currentRow += 1;
      }

      break;
    }
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
  int32_t totalElems = 0;
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
        totalElems++;
        break;
      }
    }

  }

  if (pInfo->normalized) {
    for (int32_t k = 0; k < pInfo->numOfBins; ++k) {
      if(totalElems != 0) {
        pInfo->bins[k].percentage = pInfo->bins[k].count / (double)totalElems;
      } else {
        pInfo->bins[k].percentage = 0;
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

  for (int32_t i = 0; i < pResInfo->numOfRes; ++i) {
    int32_t len;
    char buf[512] = {0};
    if (!pInfo->normalized) {
      len = sprintf(buf + VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%"PRId64"}",
                   pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].count);
    } else {
      len = sprintf(buf + VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}",
                   pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].percentage);
    }
    varDataSetLen(buf, len);
    colDataAppend(pCol, currentRow, buf, false);
    currentRow++;
  }

  return pResInfo->numOfRes;
}
