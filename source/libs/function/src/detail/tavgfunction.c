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

#include <immintrin.h>
#include "builtinsimpl.h"
#include "function.h"
#include "tdatablock.h"
#include "tfunctionInt.h"
#include "tglobal.h"

#define SET_VAL(_info, numOfElem, res) \
  do {                                 \
    if ((numOfElem) <= 0) {            \
      break;                           \
    }                                  \
    (_info)->numOfRes = (res);         \
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

typedef struct SAvgRes {
  double  result;
  SSumRes sum;
  int64_t count;
  int16_t type;  // store the original input type, used in merge function
} SAvgRes;

static int32_t handleFloatCols(const SColumnInfoData* pCol, const SInputColumnInfoData* pInput, SAvgRes* pRes) {
  int32_t numOfElems = 0;
  float*  plist = (float*)pCol->pData;

  if (pCol->hasNull || pInput->numOfRows < 8) {
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      numOfElems += 1;
      pRes->count += 1;
      pRes->sum.dsum += plist[i];
    }
  } else { // no null values exist
    numOfElems = pInput->numOfRows;
    pRes->count += pInput->numOfRows;

    // 1. an software version to speedup the process by using loop unwinding.



    // 2. if both the CPU and OS support SSE4.2, let's try the faster version by using SSE4.2 SIMD



    // 3. If both the CPU and OS support AVX, let's employ AVX instruction to speedup this loop
    // 3.1 find the start position that are aligned to 32bytes address in memory
    int32_t startElem = 0;//((uint64_t)plist) & ((1<<8u)-1);
    int32_t i = 0;

    int32_t bitWidth = 8;

    int32_t remain = (pInput->numOfRows - startElem) % bitWidth;
    int32_t rounds = (pInput->numOfRows - startElem) / bitWidth;
    const float* p = &plist[startElem];

    __m256 loadVal;
    __m256 sum = _mm256_setzero_ps();

    for(; i < rounds; ++i) {
      loadVal = _mm256_loadu_ps(p);
      sum = _mm256_add_ps(sum, loadVal);
      p += bitWidth;
    }

    // let sum up the final results
    const float* q = (const float*)&sum;
    pRes->sum.dsum += q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7];

    // calculate the front and the reminder items in array list
    for(int32_t j = 0; j < startElem; ++j) {
      pRes->sum.dsum += plist[j];
    }

    startElem += rounds * bitWidth;
    for(int32_t j = 0; j < remain; ++j) {
      pRes->sum.dsum += plist[j + startElem];
    }
  }

  return numOfElems;
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
    numOfElem = 0;
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
#if 1
        numOfElem = handleFloatCols(pCol, pInput, pAvgRes);
#else
        float* plist = (float*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.dsum += plist[i];
        }
#endif
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

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAvgRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

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