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
#include "querynodes.h"
#include "taggfunction.h"
#include "tdatablock.h"

#define SET_VAL(_info, numOfElem, res)  \
  do {                                  \
    if ((numOfElem) <= 0) {             \
      break;                            \
    }                                   \
    (_info)->numOfRes = (res);          \
    (_info)->hasResult = DATA_SET_FLAG; \
  } while (0)

typedef struct SSumRes {
  union {
    int64_t  isum;
    uint64_t usum;
    double   dsum;
  };
} SSumRes;

bool functionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return false;
  }

  if (pCtx->pOutput != NULL) {
    memset(pCtx->pOutput, 0, (size_t)pCtx->resDataInfo.bytes);
  }

  initResultRowEntry(pResultInfo, pCtx->resDataInfo.interBufSize);
  return true;
}

static void doFinalizer(SResultRowEntryInfo* pResInfo) { cleanupResultRowEntry(pResInfo); }

void functionFinalize(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  doFinalizer(pResInfo);
}

bool getCountFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

/*
 * count function does need the finalize, if data is missing, the default value, which is 0, is used
 * count function does not use the pCtx->interResBuf to keep the intermediate buffer
 */
void countFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pInputCol->hasNull == true. pInput->colDataAggIsSet == true;
   * 2. for general non-primary key columns, pInputCol->hasNull may be true or false, pInput->colDataAggIsSet == true;
   * 3. for primary key column, pInputCol->hasNull always be false, pInput->colDataAggIsSet == false;
   */
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData* pInputCol = pInput->pData[0];
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
      //when counting on the primary time stamp column and no statistics data is presented, use the size value directly.
      numOfElem = pInput->numOfRows;
    }
  }

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char* buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t *)buf) += numOfElem;

  SET_VAL(pResInfo, numOfElem, 1);
}

#define LIST_ADD_N(_res, _col, _start, _rows, _t, numOfElem)             \
  do {                                                                   \
    _t *d = (_t *)(_col->pData);                                         \
    for (int32_t i = (_start); i < (_rows) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      };                                                                 \
      (_res) += (d)[i];                                                  \
      (numOfElem)++;                                                     \
    }                                                                    \
  } while (0)

void sumFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];
  int32_t type = pInput->pData[0]->info.type;

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

    int32_t start     = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      if (type == TSDB_DATA_TYPE_TINYINT) {
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
}

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

bool maxFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  char* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  switch (pCtx->resDataInfo.type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)buf) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)buf) = 0;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)buf) = -FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
    SET_DOUBLE_VAL(((double *)buf), -DBL_MAX);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)buf) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)buf) = 0;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)buf) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)buf) = 0;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)buf) = INT8_MIN;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *((uint8_t *)buf) = 0;
      break;
    default:
      assert(0);
  }
  return true;
}

bool minFunctionSetup(SqlFunctionCtx *pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  char* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  switch (pCtx->resDataInfo.type) {
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)buf) = INT8_MAX;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t *) buf = UINT8_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)buf) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)buf) = UINT16_MAX;
      break;
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)buf) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)buf) = UINT32_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)buf) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)buf) = UINT64_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)buf) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double *)buf), DBL_MAX);
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
      SqlFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[_i];      \
      __ctx->fpSet.process(__ctx);                                 \
    }                                                              \
  } while (0);

#define DO_UPDATE_SUBSID_RES(ctx, ts)                                 \
  do {                                                                \
    for (int32_t _i = 0; _i < (ctx)->subsidiaryRes.numOfCols; ++_i) { \
      SqlFunctionCtx *__ctx = (ctx)->subsidiaryRes.pCtx[_i];          \
      if (__ctx->functionId == FUNCTION_TS_DUMMY) {                   \
        __ctx->tag.i = (ts);                                          \
        __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;                     \
      }                                                               \
      __ctx->fpSet.process(__ctx);                                    \
    }                                                                 \
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
    _t *d = (_t *)((_col)->pData);                                       \
    for (int32_t i = (_start); i < (_nrow) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      }                                                                  \
      TSKEY ts = (ctx)->ptsList != NULL ? GET_TS_DATA(ctx, i) : 0;       \
      UPDATE_DATA(ctx, val, d[i], num, sign, ts);                        \
    }                                                                    \
  } while (0)

int32_t doMinMaxHelper(SqlFunctionCtx *pCtx, int32_t isMinFunc) {
  int32_t numOfElems = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char* buf = GET_ROWCELL_INTERBUF(pResInfo);

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
      tval  = &pInput->pColumnDataAgg[0]->min;
      index = pInput->pColumnDataAgg[0]->minIndex;
    } else {
      tval  = &pInput->pColumnDataAgg[0]->max;
      index = pInput->pColumnDataAgg[0]->maxIndex;
    }

    TSKEY key = TSKEY_INITIAL_VAL;
    if (pCtx->ptsList != NULL) {
      // the index is the original position, not the relative position
      key = pCtx->ptsList[index];
    }

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t val = GET_INT64_VAL(tval);

#if defined(_DEBUG_VIEW)
      qDebug("max value updated according to pre-cal:%d", *data);
#endif

      if ((*(int64_t*)buf < val) ^ isMinFunc) {
        *(int64_t*) buf = val;
        for (int32_t i = 0; i < (pCtx)->subsidiaryRes.numOfCols; ++i) {
          SqlFunctionCtx* __ctx = pCtx->subsidiaryRes.pCtx[i];
          if (__ctx->functionId == FUNCTION_TS_DUMMY) {  // TODO refactor
            __ctx->tag.i = key;
            __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
          }

          __ctx->fpSet.process(__ctx);
        }
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t val = GET_UINT64_VAL(tval);
      UPDATE_DATA(pCtx, *(uint64_t*)buf, val, numOfElems, isMinFunc, key);
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double  val = GET_DOUBLE_VAL(tval);
      UPDATE_DATA(pCtx, *(double*)buf, val, numOfElems, isMinFunc, key);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      double val = GET_DOUBLE_VAL(tval);
      UPDATE_DATA(pCtx, *(float*)buf, (float)val, numOfElems, isMinFunc, key);
    }

    return numOfElems;
  }

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    if (type == TSDB_DATA_TYPE_TINYINT) {
      LOOPCHECK_N(*(int8_t*)buf, pCol, pCtx, int8_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      LOOPCHECK_N(*(int16_t*) buf, pCol, pCtx, int16_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_INT) {
      int32_t *pData = (int32_t*)pCol->pData;
      int32_t *val = (int32_t*) buf;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if ((*val < pData[i]) ^ isMinFunc) {
          *val = pData[i];
          TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i) : 0;
          DO_UPDATE_SUBSID_RES(pCtx, ts);
        }

        numOfElems += 1;
      }

#if defined(_DEBUG_VIEW)
      qDebug("max value updated:%d", *retVal);
#endif
    } else if (type == TSDB_DATA_TYPE_BIGINT) {
      LOOPCHECK_N(*(int64_t*) buf, pCol, pCtx, int64_t, numOfRows, start, isMinFunc, numOfElems);
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    if (type == TSDB_DATA_TYPE_UTINYINT) {
      LOOPCHECK_N(*(uint8_t*) buf, pCol, pCtx, uint8_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_USMALLINT) {
      LOOPCHECK_N(*(uint16_t*) buf, pCol, pCtx, uint16_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_UINT) {
      LOOPCHECK_N(*(uint32_t*) buf, pCol, pCtx, uint32_t, numOfRows, start, isMinFunc, numOfElems);
    } else if (type == TSDB_DATA_TYPE_UBIGINT) {
      LOOPCHECK_N(*(uint64_t*) buf, pCol, pCtx, uint64_t, numOfRows, start, isMinFunc, numOfElems);
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    LOOPCHECK_N(*(double*) buf, pCol, pCtx, double, numOfRows, start, isMinFunc, numOfElems);
  } else if (type == TSDB_DATA_TYPE_FLOAT) {
    LOOPCHECK_N(*(float*) buf, pCol, pCtx, float, numOfRows, start, isMinFunc, numOfElems);
  }

  return numOfElems;
}

void minFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElems = doMinMaxHelper(pCtx, 1);
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
}

void maxFunction(SqlFunctionCtx *pCtx) {
  int32_t numOfElems = doMinMaxHelper(pCtx, 0);
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
}

typedef struct STopBotRes {
  int32_t num;
} STopBotRes;

bool getTopBotFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
    SColumnNode* pColNode = (SColumnNode*) nodesListGetNode(pFunc->pParameterList, 0);
  int32_t bytes = pColNode->node.resType.bytes;
  SValueNode* pkNode = (SValueNode*) nodesListGetNode(pFunc->pParameterList, 1);
  return true;
}

typedef struct SStddevRes {
  int64_t count;
  union  {double  quadraticDSum; int64_t quadraticISum;};
  union  {double  dsum; int64_t isum;};
} SStddevRes;

bool getStddevFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStddevRes);
  return true;
}

void stddevFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];
  int32_t type = pInput->pData[0]->info.type;

  SStddevRes* pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

//  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start     = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    switch(type) {
      case TSDB_DATA_TYPE_INT: {
        int32_t* plist = (int32_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          pStddevRes->count += 1;
          pStddevRes->isum  += plist[i];
          pStddevRes->quadraticISum += plist[i] * plist[i];
        }
      }
      break;
    }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
}

void stddevFinalize(SqlFunctionCtx* pCtx) {
  functionFinalize(pCtx);

  SStddevRes* pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double res = pStddevRes->quadraticISum/pStddevRes->count - (pStddevRes->isum / pStddevRes->count) * (pStddevRes->isum / pStddevRes->count);
}




bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes;
  return true;
}

// TODO fix this
// This ordinary first function only handle the data block in ascending order
void firstFunction(SqlFunctionCtx *pCtx) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }

  int32_t numOfElems = 0;

  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  char* buf = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];

  // All null data column, return directly.
  if (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) {
    ASSERT(pInputCol->hasNull == true);
    return;
  }

  // Check for the first not null data
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, NULL)) {
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    memcpy(buf, data, pInputCol->info.bytes);
    // TODO handle the subsidary value
//    if (pCtx->ptsList != NULL) {
//      TSKEY k = GET_TS_DATA(pCtx, i);
//      DO_UPDATE_TAG_COLUMNS(pCtx, k);
//    }

    pResInfo->hasResult = DATA_SET_FLAG;
    pResInfo->complete = true;

    numOfElems++;
    break;
  }

  SET_VAL(pResInfo, numOfElems, 1);
}

void lastFunction(SqlFunctionCtx *pCtx) {
  if (pCtx->order != TSDB_ORDER_DESC) {
    return;
  }

  int32_t numOfElems = 0;

  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  char* buf = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];

  // All null data column, return directly.
  if (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) {
    ASSERT(pInputCol->hasNull == true);
    return;
  }

  if (pCtx->order == TSDB_ORDER_DESC) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, NULL)) {
        continue;
      }

      char* data = colDataGetData(pInputCol, i);
      memcpy(buf, data, pInputCol->info.bytes);

//      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;
//      DO_UPDATE_TAG_COLUMNS(pCtx, ts);

      pResInfo->hasResult = DATA_SET_FLAG;
      pResInfo->complete = true;  // set query completed on this column
      numOfElems++;
      break;
    }
  } else {  // ascending order
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, NULL)) {
        continue;
      }

      char* data = colDataGetData(pInputCol, i);
      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;

      if (pResInfo->hasResult != DATA_SET_FLAG || (*(TSKEY*)buf) < ts) {
        pResInfo->hasResult = DATA_SET_FLAG;
        memcpy(buf, data, pCtx->inputBytes);

        *(TSKEY*)buf = ts;
//        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
      }

      numOfElems++;
      break;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
}

void valFunction(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
  char* buf = GET_ROWCELL_INTERBUF(pResInfo);

  SColumnInfoData* pInputCol = pCtx->input.pData[0];
  memcpy(buf, pInputCol->pData, pInputCol->info.bytes);
}
