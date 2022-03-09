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
//  int8_t hasResult;
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

void functionFinalizer(SqlFunctionCtx *pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult != DATA_SET_FLAG) {
//    setNull(pCtx->pOutput, pCtx->resDataInfo.type, pCtx->resDataInfo.bytes);
  }

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
   * 1. column data missing (schema modified) causes pCtx->hasNull == true. pCtx->isAggSet == true;
   * 2. for general non-primary key columns, pCtx->hasNull may be true or false, pCtx->isAggSet == true;
   * 3. for primary key column, pCtx->hasNull always be false, pCtx->isAggSet == false;
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

static void do_sum(SqlFunctionCtx *pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg *pAgg = pInput->pColumnDataAgg[0];
  int32_t type = pInput->pData[0]->info.type;

  if (pInput->colDataAggIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;
    ASSERT(numOfElem >= 0);

    SSumRes* pSumInfo = (SSumRes*) pCtx->pOutput;
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pSumInfo->isum += pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pSumInfo->usum += pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pSumInfo->dsum += GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start     = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    SSumRes* pSum = (SSumRes*) pCtx->pOutput;

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        LIST_ADD_N(pSum->isum, pCol, start, numOfRows, int8_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(pSum->isum, pCol, start, numOfRows, int16_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(pSum->isum, pCol, start, numOfRows, int32_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(pSum->isum, pCol, start, numOfRows, int64_t, numOfElem);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
        LIST_ADD_N(pSum->usum, pCol, start, numOfRows, uint8_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
        LIST_ADD_N(pSum->usum, pCol, start, numOfRows, uint16_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
        LIST_ADD_N(pSum->usum, pCol, start, numOfRows, uint32_t, numOfElem);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
        LIST_ADD_N(pSum->usum, pCol, start, numOfRows, uint64_t, numOfElem);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N(pSum->dsum, pCol, start, numOfRows, double, numOfElem);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N(pSum->dsum, pCol, start, numOfRows, float, numOfElem);
    }
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
}

bool getSumFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

void sumFunction(SqlFunctionCtx *pCtx) {
  do_sum(pCtx);

  // keep the result data in output buffer, not in the intermediate buffer
//  SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//  if (pResInfo->hasResult == DATA_SET_FLAG) {
    // set the flag for super table query
//    SSumRes *pSum = (SSumRes *)pCtx->pOutput;
//    pSum->hasResult = DATA_SET_FLAG;
//  }
}
