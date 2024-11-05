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
#include "tcompare.h"
#include "tdatablock.h"
#include "tfunctionInt.h"
#include "tglobal.h"

#define __COMPARE_ACQUIRED_MAX(i, end, bm, _data, ctx, val, pos) \
  int32_t code = TSDB_CODE_SUCCESS;                              \
  for (; i < (end); ++i) {                                       \
    if (colDataIsNull_f(bm, i)) {                                \
      continue;                                                  \
    }                                                            \
                                                                 \
    if ((val) < (_data)[i]) {                                    \
      (val) = (_data)[i];                                        \
      if ((ctx)->subsidiaries.num > 0) {                         \
        code = updateTupleData((ctx), i, (ctx)->pSrcBlock, pos); \
        if (TSDB_CODE_SUCCESS != code) {                         \
          return code;                                           \
        }                                                        \
      }                                                          \
    }                                                            \
  }

#define __COMPARE_ACQUIRED_MIN(i, end, bm, _data, ctx, val, pos) \
  int32_t code = TSDB_CODE_SUCCESS;                              \
  for (; i < (end); ++i) {                                       \
    if (colDataIsNull_f(bm, i)) {                                \
      continue;                                                  \
    }                                                            \
                                                                 \
    if ((val) > (_data)[i]) {                                    \
      (val) = (_data)[i];                                        \
      if ((ctx)->subsidiaries.num > 0) {                         \
        code = updateTupleData((ctx), i, (ctx)->pSrcBlock, pos); \
        if (TSDB_CODE_SUCCESS != code) {                         \
          return code;                                           \
        }                                                        \
      }                                                          \
    }                                                            \
  }

#define __COMPARE_EXTRACT_MIN(start, end, val, _data) \
  for (int32_t i = (start); i < (end); ++i) {         \
    if ((val) > (_data)[i]) {                         \
      (val) = (_data)[i];                             \
    }                                                 \
  }

#define __COMPARE_EXTRACT_MAX(start, end, val, _data) \
  for (int32_t i = (start); i < (end); ++i) {         \
    if ((val) < (_data)[i]) {                         \
      (val) = (_data)[i];                             \
    }                                                 \
  }

#define GET_INVOKE_INTRINSIC_THRESHOLD(_bits, _bytes) ((_bits) / ((_bytes) << 3u))

static int32_t findFirstValPosition(const SColumnInfoData* pCol, int32_t start, int32_t numOfRows, bool isStr) {
  int32_t i = start;

  while (i < (start + numOfRows) && (isStr ? colDataIsNull_s(pCol, i) : colDataIsNull_f(pCol->nullbitmap, i) == true)) {
    i += 1;
  }

  return i;
}

static void handleInt8Col(const void* data, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf, bool isMinFunc,
                          bool signVal) {
  if (!pBuf->assign) {
    pBuf->v = ((const int8_t*)data)[start];
  }

  if (tsAVX2Supported && tsSIMDEnable && numOfRows * sizeof(int8_t) >= M256_BYTES) {
    int32_t code = i8VectorCmpAVX2(((char*)data) + start * sizeof(int8_t), numOfRows, isMinFunc, signVal, &pBuf->v);
    if (code == TSDB_CODE_SUCCESS) {
      pBuf->assign = true;
      return;
    }
  }

  if (signVal) {
    const int8_t* p = (const int8_t*)data;
    int8_t*       v = (int8_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  } else {
    const uint8_t* p = (const uint8_t*)data;
    uint8_t*       v = (uint8_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  }

  pBuf->assign = true;
}

static void handleInt16Col(const void* data, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf, bool isMinFunc,
                           bool signVal) {
  if (!pBuf->assign) {
    pBuf->v = ((const int16_t*)data)[start];
  }

  if (tsAVX2Supported && tsSIMDEnable && numOfRows * sizeof(int16_t) >= M256_BYTES) {
    int32_t code = i16VectorCmpAVX2(((char*)data) + start * sizeof(int16_t), numOfRows, isMinFunc, signVal, &pBuf->v);
    if (code == TSDB_CODE_SUCCESS) {
      pBuf->assign = true;
      return;
    }
  }

  if (signVal) {
    const int16_t* p = (const int16_t*)data;
    int16_t*       v = (int16_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  } else {
    const uint16_t* p = (const uint16_t*)data;
    uint16_t*       v = (uint16_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  }

  pBuf->assign = true;
}

static void handleInt32Col(const void* data, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf, bool isMinFunc,
                           bool signVal) {
  if (!pBuf->assign) {
    pBuf->v = ((const int32_t*)data)[start];
  }

  if (tsAVX2Supported && tsSIMDEnable && numOfRows * sizeof(int32_t) >= M256_BYTES) {
    int32_t code = i32VectorCmpAVX2(((char*)data) + start * sizeof(int32_t), numOfRows, isMinFunc, signVal, &pBuf->v);
    if (code == TSDB_CODE_SUCCESS) {
      pBuf->assign = true;
      return;
    }
  }

  if (signVal) {
    const int32_t* p = (const int32_t*)data;
    int32_t*       v = (int32_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  } else {
    const uint32_t* p = (const uint32_t*)data;
    uint32_t*       v = (uint32_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  }

  pBuf->assign = true;
}

static void handleInt64Col(const void* data, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf, bool isMinFunc,
                           bool signVal) {
  if (!pBuf->assign) {
    pBuf->v = ((const int64_t*)data)[start];
  }

  if (signVal) {
    const int64_t* p = (const int64_t*)data;
    int64_t*       v = &pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  } else {
    const uint64_t* p = (const uint64_t*)data;
    uint64_t*       v = (uint64_t*)&pBuf->v;

    if (isMinFunc) {
      __COMPARE_EXTRACT_MIN(start, start + numOfRows, *v, p);
    } else {
      __COMPARE_EXTRACT_MAX(start, start + numOfRows, *v, p);
    }
  }

  pBuf->assign = true;
}

static void handleFloatCol(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf,
                           bool isMinFunc) {
  float* pData = (float*)pCol->pData;
  float* val = (float*)&pBuf->v;
  if (!pBuf->assign) {
    *val = pData[start];
  }

  if (tsAVX2Supported && tsSIMDEnable && numOfRows * sizeof(float) >= M256_BYTES) {
    int32_t code = floatVectorCmpAVX2(pData + start, numOfRows, isMinFunc, val);
    if (code == TSDB_CODE_SUCCESS) {
      pBuf->assign = true;
      return;
    }
  }

  if (isMinFunc) {  // min
    __COMPARE_EXTRACT_MIN(start, start + numOfRows, *val, pData);
  } else {  // max
    __COMPARE_EXTRACT_MAX(start, start + numOfRows, *val, pData);
  }

  pBuf->assign = true;
}

static void handleDoubleCol(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SMinmaxResInfo* pBuf,
                            bool isMinFunc) {
  double* pData = (double*)pCol->pData;
  double* val = (double*)&pBuf->v;
  if (!pBuf->assign) {
    *val = pData[start];
  }

  if (tsAVX2Supported && tsSIMDEnable && numOfRows * sizeof(double) >= M256_BYTES) {
    int32_t code = doubleVectorCmpAVX2(pData + start, numOfRows, isMinFunc, val);
    if (code == TSDB_CODE_SUCCESS) {
      pBuf->assign = true;
      return;
    }
  }

  if (isMinFunc) {  // min
    __COMPARE_EXTRACT_MIN(start, start + numOfRows, *val, pData);
  } else {  // max
    __COMPARE_EXTRACT_MAX(start, start + numOfRows, *val, pData);
  }

  pBuf->assign = true;
}

static int32_t findRowIndex(int32_t start, int32_t num, SColumnInfoData* pCol, const char* tval) {
  // the data is loaded, not only the block SMA value
  for (int32_t i = start; i < num + start; ++i) {
    char* p = colDataGetData(pCol, i);
    if (memcmp((void*)tval, p, pCol->info.bytes) == 0) {
      return i;
    }
  }

  // if reach here means real data of block SMA is not set in pCtx->input.
  return -1;
}

static int32_t doExtractVal(SColumnInfoData* pCol, int32_t i, int32_t end, SqlFunctionCtx* pCtx, SMinmaxResInfo* pBuf,
                            bool isMinFunc) {
  if (isMinFunc) {
    switch (pCol->info.type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT: {
        const int8_t* pData = (const int8_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(int8_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_SMALLINT: {
        const int16_t* pData = (const int16_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(int16_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_INT: {
        const int32_t* pData = (const int32_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(int32_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_BIGINT: {
        const int64_t* pData = (const int64_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, (pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UTINYINT: {
        const uint8_t* pData = (const uint8_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(uint8_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_USMALLINT: {
        const uint16_t* pData = (const uint16_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(uint16_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UINT: {
        const uint32_t* pData = (const uint32_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(uint32_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UBIGINT: {
        const uint64_t* pData = (const uint64_t*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(uint64_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_FLOAT: {
        const float* pData = (const float*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(float*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        const double* pData = (const double*)pCol->pData;
        __COMPARE_ACQUIRED_MIN(i, end, pCol->nullbitmap, pData, pCtx, *(double*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY: {
        int32_t code = TSDB_CODE_SUCCESS;
        for (; i < (end); ++i) {
          if (colDataIsNull_var(pCol, i)) {
            continue;
          }
          char* pLeft = (char*)colDataGetData(pCol, i);
          char* pRight = (char*)pBuf->str;

          int32_t ret = compareLenBinaryVal(pLeft, pRight);
          if (ret < 0) {
            memcpy(pBuf->str, pLeft, varDataTLen(pLeft));
            if (pCtx->subsidiaries.num > 0) {
              code = updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              if (TSDB_CODE_SUCCESS != code) {
                return code;
              }
            }
          }
        }
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t code = TSDB_CODE_SUCCESS;
        for (; i < (end); ++i) {
          if (colDataIsNull_var(pCol, i)) {
            continue;
          }
          char* pLeft = (char*)colDataGetData(pCol, i);
          char* pRight = (char*)pBuf->str;

          int32_t ret = compareLenPrefixedWStr(pLeft, pRight);
          if (ret < 0) {
            memcpy(pBuf->str, pLeft, varDataTLen(pLeft));
            if (pCtx->subsidiaries.num > 0) {
              code = updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              if (TSDB_CODE_SUCCESS != code) {
                return code;
              }
            }
          }
        }
        break;
      }
    }
  } else {
    switch (pCol->info.type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT: {
        const int8_t* pData = (const int8_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(int8_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_SMALLINT: {
        const int16_t* pData = (const int16_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(int16_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_INT: {
        const int32_t* pData = (const int32_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(int32_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_BIGINT: {
        const int64_t* pData = (const int64_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, (pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UTINYINT: {
        const uint8_t* pData = (const uint8_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(uint8_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_USMALLINT: {
        const uint16_t* pData = (const uint16_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(uint16_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UINT: {
        const uint32_t* pData = (const uint32_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(uint32_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_UBIGINT: {
        const uint64_t* pData = (const uint64_t*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(uint64_t*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_FLOAT: {
        const float* pData = (const float*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(float*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        const double* pData = (const double*)pCol->pData;
        __COMPARE_ACQUIRED_MAX(i, end, pCol->nullbitmap, pData, pCtx, *(double*)&(pBuf->v), &pBuf->tuplePos)
        break;
      }

      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY: {
        int32_t code = TSDB_CODE_SUCCESS;
        for (; i < (end); ++i) {
          if (colDataIsNull_var(pCol, i)) {
            continue;
          }
          char* pLeft = (char*)colDataGetData(pCol, i);
          char* pRight = (char*)pBuf->str;

          int32_t ret = compareLenBinaryVal(pLeft, pRight);
          if (ret > 0) {
            memcpy(pBuf->str, pLeft, varDataTLen(pLeft));
            if (pCtx->subsidiaries.num > 0) {
              code = updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              if (TSDB_CODE_SUCCESS != code) {
                return code;
              }
            }
          }
        }
        break;
      }

      case TSDB_DATA_TYPE_NCHAR: {
        int32_t code = TSDB_CODE_SUCCESS;
        for (; i < (end); ++i) {
          if (colDataIsNull_var(pCol, i)) {
            continue;
          }
          char* pLeft = (char*)colDataGetData(pCol, i);
          char* pRight = (char*)pBuf->str;

          int32_t ret = compareLenPrefixedWStr(pLeft, pRight);
          if (ret > 0) {
            memcpy(pBuf->str, pLeft, varDataTLen(pLeft));
            if (pCtx->subsidiaries.num > 0) {
              code = updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              if (TSDB_CODE_SUCCESS != code) {
                return code;
              }
            }
          }
        }
        break;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t saveRelatedTupleTag(SqlFunctionCtx* pCtx, SInputColumnInfoData* pInput, void* tval) {
  SColumnInfoData* pCol = pInput->pData[0];

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pBuf = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  if (pCtx->subsidiaries.num > 0) {
    code = saveTupleData(pCtx, 0, pCtx->pSrcBlock, &pBuf->tuplePos);
  }
  return code;
}

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc, int32_t* nElems) {
  int32_t numOfElems = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pBuf = GET_ROWCELL_INTERBUF(pResInfo);
  pBuf->type = type;

  if (IS_NULL_TYPE(type)) {
    goto _over;
  }

  // data in current data block are qualified to the query
  if (pInput->colDataSMAIsSet && !IS_STR_DATA_TYPE(type)) {
    numOfElems = pInput->numOfRows - pAgg->numOfNull;
    if (numOfElems == 0) {
      goto _over;
    }

    int16_t index = 0;
    void*   tval = (isMinFunc) ? &pInput->pColumnDataAgg[0]->min : &pInput->pColumnDataAgg[0]->max;

    if (!pBuf->assign) {
      if (type == TSDB_DATA_TYPE_FLOAT) {
        GET_FLOAT_VAL(&pBuf->v) = GET_DOUBLE_VAL(tval);
      } else {
        pBuf->v = GET_INT64_VAL(tval);
      }

      code = saveRelatedTupleTag(pCtx, pInput, tval);
    } else {
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        int64_t prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        int64_t val = GET_INT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          GET_INT64_VAL(&pBuf->v) = val;
          code = saveRelatedTupleTag(pCtx, pInput, tval);
        }
      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        uint64_t prev = 0;
        GET_TYPED_DATA(prev, uint64_t, type, &pBuf->v);

        uint64_t val = GET_UINT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          GET_UINT64_VAL(&pBuf->v) = val;
          code = saveRelatedTupleTag(pCtx, pInput, tval);
        }
      } else if (type == TSDB_DATA_TYPE_DOUBLE) {
        double prev = 0;
        GET_TYPED_DATA(prev, double, type, &pBuf->v);

        double val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          GET_DOUBLE_VAL(&pBuf->v) = val;
          code = saveRelatedTupleTag(pCtx, pInput, tval);
        }
      } else if (type == TSDB_DATA_TYPE_FLOAT) {
        float prev = 0;
        GET_TYPED_DATA(prev, float, type, &pBuf->v);

        float val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          GET_FLOAT_VAL(&pBuf->v) = val;
          code = saveRelatedTupleTag(pCtx, pInput, tval);
        }
      }
    }

    numOfElems = 1;
    pBuf->assign = true;
    goto _over;
  }

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;
  int32_t end = start + numOfRows;

  // clang-format off
  int32_t threshold[] = {
      //NULL,    BOOL,      TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, VARCHAR,   TIMESTAMP, NCHAR,
      INT32_MAX, INT32_MAX, 32,      16,       8,   4,      8,     4,      INT32_MAX, INT32_MAX, INT32_MAX,
      // UTINYINT,USMALLINT, UINT, UBIGINT,   JSON,      VARBINARY, DECIMAL,   BLOB,      MEDIUMBLOB, BINARY
      32,         16,        8,    4,         INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX,  INT32_MAX,
  };
  // clang-format on

  if (pCol->hasNull || numOfRows < threshold[pCol->info.type] || pCtx->subsidiaries.num > 0) {
    int32_t i = findFirstValPosition(pCol, start, numOfRows, IS_STR_DATA_TYPE(type));

    if ((i < end) && (!pBuf->assign)) {
      char* p = pCol->pData + pCol->info.bytes * i;

      switch (type) {
        case TSDB_DATA_TYPE_DOUBLE:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_BIGINT:
          pBuf->v = *(int64_t*)p;
          break;
        case TSDB_DATA_TYPE_UINT:
        case TSDB_DATA_TYPE_INT:
          pBuf->v = *(int32_t*)p;
          break;
        case TSDB_DATA_TYPE_USMALLINT:
        case TSDB_DATA_TYPE_SMALLINT:
          pBuf->v = *(int16_t*)p;
          break;
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_UTINYINT:
        case TSDB_DATA_TYPE_TINYINT:
          pBuf->v = *(int8_t*)p;
          break;
        case TSDB_DATA_TYPE_FLOAT: {
          *(float*)&pBuf->v = *(float*)p;
          break;
        }
        case TSDB_DATA_TYPE_VARCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_NCHAR: {
          pBuf->str = taosMemoryMalloc(pCol->info.bytes);
          if (pBuf->str == NULL) {
            return terrno;
          }
          (void)memcpy(pBuf->str, colDataGetData(pCol, i), varDataTLen(colDataGetData(pCol, i)));
          break;
        }
        default:
          (void)memcpy(&pBuf->v, p, pCol->info.bytes);
          break;
      }

      if (pCtx->subsidiaries.num > 0) {
        code = saveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      pBuf->assign = true;
      numOfElems = 1;
    }

    if (i >= end) {
      goto _over;
    }

    code = doExtractVal(pCol, i, end, pCtx, pBuf, isMinFunc);
  } else {
    numOfElems = numOfRows;

    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT: {
        handleInt8Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, true);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        handleInt16Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, true);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        handleInt32Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, true);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        handleInt64Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, true);
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        handleInt8Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, false);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        handleInt16Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, false);
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        handleInt32Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, false);
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        handleInt64Col(pCol->pData, start, numOfRows, pBuf, isMinFunc, false);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        handleFloatCol(pCol, start, numOfRows, pBuf, isMinFunc);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        handleDoubleCol(pCol, start, numOfRows, pBuf, isMinFunc);
        break;
      }
    }

    pBuf->assign = true;
  }

_over:
  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pBuf->nullTupleSaved) {
    code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pBuf->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pBuf->nullTupleSaved = true;
  }

  *nElems = numOfElems;
  return code;
}
