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
      if (colDataIsNull_f(pCol, i)) {                         \
        continue;                                                         \
      }                                                                   \
                                                                          \
      numOfElem += 1;                                                     \
      pAvgRes->count -= 1;                                                \
      sumT -= plist[i];                                                   \
    }                                                                     \
  } while (0)

// define signed number sum with check overflow
#define CHECK_OVERFLOW_SUM_SIGNED(pAvgRes, val)                                    \
  do {                                                                             \
    SAvgRes* out = pAvgRes;                                                        \
    if (out->sum.overflow) {                                                       \
      out->sum.dsum += val;                                                        \
    } else if (out->sum.isum > 0 && val > 0 && INT64_MAX - out->sum.isum <= val || \
               out->sum.isum < 0 && val < 0 && INT64_MIN - out->sum.isum >= val) { \
      double dsum = (double)out->sum.isum;                                         \
      out->sum.overflow = true;                                                    \
      out->sum.dsum = dsum + val;                                                  \
    } else {                                                                       \
      out->sum.isum += val;                                                        \
    }                                                                              \
  } while (0)

// val is big than INT64_MAX, val come from merge
#define CHECK_OVERFLOW_SUM_SIGNED_BIG(pAvgRes, val, big)                                  \
  do {                                                                                    \
    SAvgRes* out = pAvgRes;                                                               \
    if (out->sum.overflow) {                                                              \
      out->sum.dsum += val;                                                               \
    } else if (out->sum.isum > 0 && val > 0 && INT64_MAX - out->sum.isum <= val ||        \
               out->sum.isum < 0 && val < 0 && INT64_MIN - out->sum.isum >= val || big) { \
      double dsum = (double)out->sum.isum;                                                \
      out->sum.overflow = true;                                                           \
      out->sum.dsum = dsum + val;                                                         \
    } else {                                                                              \
      SUM_RES_INC_ISUM(&AVG_RES_GET_SUM(out), val);                                       \
    }                                                                                     \
  } while (0)

// define unsigned number sum with check overflow
#define CHECK_OVERFLOW_SUM_UNSIGNED(pAvgRes, val)   \
  do {                                              \
    SAvgRes* out = pAvgRes;                         \
    if (out->sum.overflow) {                        \
      out->sum.dsum += val;                         \
    } else if (UINT64_MAX - out->sum.usum <= val) { \
      double dsum = (double)out->sum.usum;          \
      out->sum.overflow = true;                     \
      out->sum.dsum = dsum + val;                   \
    } else {                                        \
      out->sum.usum += val;                         \
    }                                               \
  } while (0)

// val is big than UINT64_MAX, val come from merge
#define CHECK_OVERFLOW_SUM_UNSIGNED_BIG(pAvgRes, val, big) \
  do {                                                     \
    SAvgRes* out = pAvgRes;                                \
    if (out->sum.overflow) {                               \
      out->sum.dsum += val;                                \
    } else if (UINT64_MAX - out->sum.usum <= val || big) { \
      double dsum = (double)out->sum.usum;                 \
      out->sum.overflow = true;                            \
      out->sum.dsum = dsum + val;                          \
    } else {                                               \
      out->sum.usum += val;                                \
    }                                                      \
  } while (0)

int32_t getAvgInfoSize(SFunctionNode* pFunc) {
  if (pFunc->pSrcFuncRef) return AVG_RES_GET_SIZE(pFunc->pSrcFuncRef->srcFuncInputType.type);
  return AVG_RES_GET_SIZE(pFunc->srcFuncInputType.type);
}

bool getAvgFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize =getAvgInfoSize(pFunc);
  return true;
}

int32_t avgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS != functionSetup(pCtx, pResultInfo)) {
    return TSDB_CODE_FUNC_SETUP_ERROR;
  }

  void* pRes = GET_ROWCELL_INTERBUF(pResultInfo);
  (void)memset(pRes, 0, pCtx->resDataInfo.interBufSize);
  return TSDB_CODE_SUCCESS;
}

static int32_t calculateAvgBySMAInfo(void* pRes, int32_t numOfRows, int32_t type, const SColumnDataAgg* pAgg, int32_t* pNumOfElem) {
  int32_t numOfElem = numOfRows - pAgg->numOfNull;

  AVG_RES_INC_COUNT(pRes, type, numOfElem);
  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    CHECK_OVERFLOW_SUM_SIGNED(pRes, pAgg->sum);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    CHECK_OVERFLOW_SUM_UNSIGNED(pRes, pAgg->sum);
  } else if (IS_FLOAT_TYPE(type)) {
    SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pRes), GET_DOUBLE_VAL((const char*)&(pAgg->sum)));
  } else if (IS_DECIMAL_TYPE(type)) {
    bool overflow = pAgg->overflow;
    if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
    SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pRes), &pAgg->decimal128Sum, TSDB_DATA_TYPE_DECIMAL);
    if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
  }

  *pNumOfElem = numOfElem;
  return 0;
}

static int32_t doAddNumericVector(SColumnInfoData* pCol, int32_t type, SInputColumnInfoData *pInput, void* pRes, int32_t* pNumOfElem) {
  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;
  int32_t numOfElems = 0;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* plist = (int8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_TINYINT, 1);
        CHECK_OVERFLOW_SUM_SIGNED(pRes, plist[i]);
      }

      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* plist = (int16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_SMALLINT, 1);
        CHECK_OVERFLOW_SUM_SIGNED(pRes, plist[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* plist = (int32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_INT, 1);
        CHECK_OVERFLOW_SUM_SIGNED(pRes, plist[i]);
      }

      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* plist = (int64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_BIGINT, 1);
        CHECK_OVERFLOW_SUM_SIGNED(pRes, plist[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* plist = (uint8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_UTINYINT, 1);
        CHECK_OVERFLOW_SUM_UNSIGNED(pRes, plist[i]);
      }

      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* plist = (uint16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_USMALLINT, 1);
        CHECK_OVERFLOW_SUM_UNSIGNED(pRes, plist[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t* plist = (uint32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_UINT, 1);
        CHECK_OVERFLOW_SUM_UNSIGNED(pRes, plist[i]);
      }

      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* plist = (uint64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_UBIGINT, 1);
        CHECK_OVERFLOW_SUM_UNSIGNED(pRes, plist[i]);
        
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float* plist = (float*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_FLOAT, 1);
        SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pRes), plist[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* plist = (double*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, TSDB_DATA_TYPE_DOUBLE, 1);
        SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pRes), plist[i]);
      }
      break;
    }
    case TSDB_DATA_TYPE_DECIMAL64:
    case TSDB_DATA_TYPE_DECIMAL: {
      const char* pDec = pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol, i)) {
          continue;
        }

        numOfElems += 1;
        AVG_RES_INC_COUNT(pRes, type, 1);
        bool overflow = false;
        SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pRes), (const void*)(pDec + i * tDataTypes[type].bytes), type);
        if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
      }
    } break;
    default:
      break;
  }

  *pNumOfElem = numOfElems;
  return 0;
}

int32_t avgFunction(SqlFunctionCtx* pCtx) {
  int32_t       numOfElem = 0;
  const int32_t THRESHOLD_SIZE = 8;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;
  pCtx->inputType = type;

  void* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(type)) {
    goto _over;
  }

  AVG_RES_SET_TYPE(pAvgRes, pCtx->inputType, type);
  if (IS_DECIMAL_TYPE(type)) AVG_RES_SET_INPUT_SCALE(pAvgRes, pInput->pData[0]->info.scale);

  if (pInput->colDataSMAIsSet) {  // try to use SMA if available
    int32_t code = calculateAvgBySMAInfo(pAvgRes, numOfRows, type, pAgg, &numOfElem);
    if (code != 0) return code;
  } else if (!pCol->hasNull) {  // try to employ the simd instructions to speed up the loop
    numOfElem = pInput->numOfRows;
    AVG_RES_INC_COUNT(pAvgRes, pCtx->inputType, pInput->numOfRows);

    switch(type) {
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT: {
        const int8_t* plist = (const int8_t*) pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          if (type == TSDB_DATA_TYPE_TINYINT) {
            CHECK_OVERFLOW_SUM_SIGNED(pAvgRes, plist[i]);
          } else {
            CHECK_OVERFLOW_SUM_UNSIGNED(pAvgRes, (uint8_t)plist[i]);
          }
        }
        break;
      }

      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT: {
        const int16_t* plist = (const int16_t*)pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          if (type == TSDB_DATA_TYPE_SMALLINT) {
            CHECK_OVERFLOW_SUM_SIGNED(pAvgRes, plist[i]);
          } else {
            CHECK_OVERFLOW_SUM_UNSIGNED(pAvgRes, (uint16_t)plist[i]);
          }
        }
        break;
      }

      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT: {
        const int32_t* plist = (const int32_t*) pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          if (type == TSDB_DATA_TYPE_INT) {
            CHECK_OVERFLOW_SUM_SIGNED(pAvgRes, plist[i]);
          } else {
            CHECK_OVERFLOW_SUM_UNSIGNED(pAvgRes, (uint32_t)plist[i]);
          }
        }
        break;
      }

      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT: {
        const int64_t* plist = (const int64_t*) pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          if (type == TSDB_DATA_TYPE_BIGINT) {
            CHECK_OVERFLOW_SUM_SIGNED(pAvgRes, plist[i]);
          } else {
            CHECK_OVERFLOW_SUM_UNSIGNED(pAvgRes, (uint64_t)plist[i]);
          }
        }
        break;
      }

      case TSDB_DATA_TYPE_FLOAT: {
        const float* plist = (const float*) pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pAvgRes), plist[i]);
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        const double* plist = (const double*)pCol->pData;

        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pAvgRes), plist[i]);
        }
        break;
      }
      case TSDB_DATA_TYPE_DECIMAL:
      case TSDB_DATA_TYPE_DECIMAL64: {
        const char* pDec = pCol->pData;
        for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
          bool overflow = false;
          if (type == TSDB_DATA_TYPE_DECIMAL64) {
            SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pAvgRes), (const void*)(pDec + i * tDataTypes[type].bytes),
                                    TSDB_DATA_TYPE_DECIMAL64);
          } else {
            SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pAvgRes), (const void*)(pDec + i * tDataTypes[type].bytes),
                                    TSDB_DATA_TYPE_DECIMAL);
          }
          if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
        }
      } break;
      default:
        return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
    }
  } else {
    int32_t code = doAddNumericVector(pCol, type, pInput, pAvgRes, &numOfElem);
    if (code) return code;
  }

_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t avgTransferInfo(SqlFunctionCtx* pCtx, void* pInput, void* pOutput) {
  int32_t inputDT = pCtx->pExpr->pExpr->_function.pFunctNode->srcFuncInputType.type;
  int32_t type = AVG_RES_GET_TYPE(pInput, inputDT);
  pCtx->inputType = type;
  if (IS_NULL_TYPE(type)) {
    return 0;
  }


  AVG_RES_SET_TYPE(pOutput, inputDT, type);
  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    bool overflow = AVG_RES_GET_SUM_OVERFLOW(pInput, false, 0);
    CHECK_OVERFLOW_SUM_SIGNED_BIG(pOutput, (overflow ? SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pInput)) : SUM_RES_GET_ISUM(&AVG_RES_GET_SUM(pInput))), overflow);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    bool overflow = AVG_RES_GET_SUM_OVERFLOW(pInput, false, 0);
    CHECK_OVERFLOW_SUM_UNSIGNED_BIG(pOutput, (overflow ? SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pInput)) : SUM_RES_GET_USUM(&AVG_RES_GET_SUM(pInput))), overflow);
  } else if (IS_DECIMAL_TYPE(type)) {
    AVG_RES_SET_INPUT_SCALE(pOutput, AVG_RES_GET_INPUT_SCALE(pInput));
    bool overflow = false;
    SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pOutput), &AVG_RES_GET_DECIMAL_SUM(pInput), TSDB_DATA_TYPE_DECIMAL);
    if (overflow) return TSDB_CODE_DECIMAL_OVERFLOW;
  } else {
    SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pOutput), SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pInput)));
  }

  AVG_RES_INC_COUNT(pOutput, type, AVG_RES_GET_COUNT(pInput, true, type));
  return 0;
}

int32_t avgFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (IS_NULL_TYPE(pCol->info.type)) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
    return TSDB_CODE_SUCCESS;
  }

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  void* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    if(colDataIsNull_s(pCol, i)) continue;
    char*    data = colDataGetData(pCol, i);
    void* pInputInfo = varDataVal(data);
    int32_t code = avgTransferInfo(pCtx, pInputInfo, pInfo);
    if (code != 0) return code;
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);

  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t avgInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SAvgRes* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (pCol->info.type) {
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
#endif

int32_t avgCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  void*                pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = AVG_RES_GET_TYPE(pDBuf, pDestCtx->inputType);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  void*                pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  type = (type == TSDB_DATA_TYPE_NULL) ? AVG_RES_GET_TYPE(pSBuf, pDestCtx->inputType) : type;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    CHECK_OVERFLOW_SUM_SIGNED(pDBuf, SUM_RES_GET_ISUM(&AVG_RES_GET_SUM(pSBuf)));
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    CHECK_OVERFLOW_SUM_UNSIGNED(pDBuf, SUM_RES_GET_USUM(&AVG_RES_GET_SUM(pSBuf)));
  } else if (IS_DECIMAL_TYPE(type)) {
    bool overflow = false;
    SUM_RES_INC_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pDBuf), &SUM_RES_GET_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pSBuf)), type);
    if (overflow) {

    }
  } else {
    SUM_RES_INC_DSUM(&AVG_RES_GET_SUM(pDBuf), SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pSBuf)));
  }
  AVG_RES_INC_COUNT(pDBuf, pDestCtx->inputType, AVG_RES_GET_COUNT(pSBuf, true, pDestCtx->inputType));

  return TSDB_CODE_SUCCESS;
}

int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  void*   pRes = GET_ROWCELL_INTERBUF(pEntryInfo);
  int32_t type = AVG_RES_GET_TYPE(pRes, pCtx->inputType);
  int64_t count = AVG_RES_GET_COUNT(pRes, true, type);

  if (AVG_RES_GET_COUNT(pRes, true, pCtx->inputType) > 0) {
    
    if(AVG_RES_GET_SUM_OVERFLOW(pRes, true, pCtx->inputType)) {
      AVG_RES_GET_AVG(pRes) = SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pRes)) / ((double)AVG_RES_GET_COUNT(pRes, false, 0));
    }else if (IS_SIGNED_NUMERIC_TYPE(type)) {
      AVG_RES_GET_AVG(pRes) = SUM_RES_GET_ISUM(&AVG_RES_GET_SUM(pRes)) / ((double)AVG_RES_GET_COUNT(pRes, false, 0));
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      AVG_RES_GET_AVG(pRes) = SUM_RES_GET_USUM(&AVG_RES_GET_SUM(pRes)) / ((double)AVG_RES_GET_COUNT(pRes, false, 0));
    } else if (IS_DECIMAL_TYPE(type)) {
      int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      SDataType        sumDt = {.type = TSDB_DATA_TYPE_DECIMAL,
                                .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                                .precision = pCol->info.precision,
                                .scale = AVG_RES_GET_INPUT_SCALE(pRes)};
      SDataType        countDt = {
                 .type = TSDB_DATA_TYPE_BIGINT, .bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes, .precision = 0, .scale = 0};
      SDataType avgDt = {.type = TSDB_DATA_TYPE_DECIMAL,
                         .bytes = tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes,
                         .precision = pCol->info.precision,
                         .scale = pCol->info.scale};
      int64_t   count = AVG_RES_GET_COUNT(pRes, true, type);
      int32_t   code =
          decimalOp(OP_TYPE_DIV, &sumDt, &countDt, &avgDt, &SUM_RES_GET_DECIMAL_SUM(&AVG_RES_GET_DECIMAL_SUM(pRes)),
                    &count, &AVG_RES_GET_DECIMAL_AVG(pRes));
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      AVG_RES_GET_AVG(pRes) = SUM_RES_GET_DSUM(&AVG_RES_GET_SUM(pRes)) / ((double)AVG_RES_GET_COUNT(pRes, false, 0));
    }
  }
  if (AVG_RES_GET_COUNT(pRes, true, pCtx->inputType) == 0) {
    pEntryInfo->numOfRes = 0;
  } else if (!IS_DECIMAL_TYPE(pCtx->inputType)) {
    if (isinf(AVG_RES_GET_AVG(pRes)) || isnan(AVG_RES_GET_AVG(pRes))) pEntryInfo->numOfRes = 0;
  } else {
    pEntryInfo->numOfRes = 1;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t avgPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  void*                pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = AVG_RES_GET_SIZE(pCtx->inputType);
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));
  int32_t              code = TSDB_CODE_SUCCESS;
  if (NULL == res) {
    return terrno;
  }
  (void)memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  if(NULL == pCol) {
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

  code = colDataSetVal(pCol, pBlock->info.rows, res, false);

_exit:
  taosMemoryFree(res);
  return code;
}
