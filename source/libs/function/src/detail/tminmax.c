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

static int32_t handleInt32Col(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                              SMinmaxResInfo* pBuf, bool isMinFunc) {
  int32_t* pData = (int32_t*)pCol->pData;
  int32_t* val = (int32_t*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows < 8 || pCtx->subsidiaries.num > 0) {
    if (isMinFunc) {  // min
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          if (*val > pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else {  // max function
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
        }

        numOfElems += 1;
      }
    }
  } else { // not has null value
    // 1. software version




    // 3. AVX2 version to speedup the loop
    int32_t startElem = 0;//((uint64_t)plist) & ((1<<8u)-1);
    int32_t i = 0;

    int32_t bitWidth = 8;
    int32_t v = 0;

    int32_t remain = (numOfRows - startElem) % bitWidth;
    int32_t rounds = (numOfRows - startElem) / bitWidth;
    const int32_t* p = &pData[startElem];

    __m256i next;
    __m256i initialVal = _mm256_loadu_si256((__m256i*)p);
    p += bitWidth;

    if (!isMinFunc) {  // max function
      for (; i < rounds; ++i) {
        next = _mm256_loadu_si256((__m256i*)p);
        initialVal = _mm256_max_epi32(initialVal, next);
        p += bitWidth;
      }

      // let sum up the final results
      const int32_t* q = (const int32_t*)&initialVal;

      v = TMAX(q[0], q[1]);
      v = TMAX(v, q[2]);
      v = TMAX(v, q[3]);
      v = TMAX(v, q[4]);
      v = TMAX(v, q[5]);
      v = TMAX(v, q[6]);
      v = TMAX(v, q[7]);

      // calculate the front and the reminder items in array list
      startElem += rounds * bitWidth;
      for (int32_t j = 0; j < remain; ++j) {
        if (v < p[j + startElem]) {
          v = p[j + startElem];
        }
      }
    } else {  // min function
      for (; i < rounds; ++i) {
        next = _mm256_loadu_si256((__m256i*)p);
        initialVal = _mm256_min_epi32(initialVal, next);
        p += bitWidth;
      }

      // let sum up the final results
      const int32_t* q = (const int32_t*)&initialVal;

      v = TMIN(q[0], q[1]);
      v = TMIN(v, q[2]);
      v = TMIN(v, q[3]);
      v = TMIN(v, q[4]);
      v = TMIN(v, q[5]);
      v = TMIN(v, q[6]);
      v = TMIN(v, q[7]);

      // calculate the front and the reminder items in array list
      startElem += rounds * bitWidth;
      for (int32_t j = 0; j < remain; ++j) {
        if (v > p[j + startElem]) {
          v = p[j + startElem];
        }
      }
    }

    *val = v;
    numOfElems = numOfRows;
  }

  return numOfElems;
}

static int32_t handleFloatCol(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                              SMinmaxResInfo* pBuf, bool isMinFunc) {
  float* pData = (float*)pCol->pData;
  double* val = (double*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows < 8 || pCtx->subsidiaries.num > 0) {
    if (isMinFunc) {  // min
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          if (*val > pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else {  // max function
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (*val < pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    }
  } else { // not has null value
    // 1. software version




    // 3. AVX2 version to speedup the loop
    int32_t startElem = 0;//((uint64_t)plist) & ((1<<8u)-1);
    int32_t i = 0;

    int32_t bitWidth = 8;
    float v = 0;

    int32_t remain = (numOfRows - startElem) % bitWidth;
    int32_t rounds = (numOfRows - startElem) / bitWidth;
    const float* p = &pData[startElem];

    __m256 next;
    __m256 initialVal = _mm256_loadu_ps(p);
    p += bitWidth;

    if (!isMinFunc) {  // max function
      for (; i < rounds; ++i) {
        next = _mm256_loadu_ps(p);
        initialVal = _mm256_max_ps(initialVal, next);
        p += bitWidth;
      }

      // let sum up the final results
      const float* q = (const float*)&initialVal;

      v = TMAX(q[0], q[1]);
      v = TMAX(v, q[2]);
      v = TMAX(v, q[3]);
      v = TMAX(v, q[4]);
      v = TMAX(v, q[5]);
      v = TMAX(v, q[6]);
      v = TMAX(v, q[7]);

      // calculate the front and the reminder items in array list
      startElem += rounds * bitWidth;
      for (int32_t j = 0; j < remain; ++j) {
        if (v < p[j + startElem]) {
          v = p[j + startElem];
        }
      }
    } else {  // min function
      for (; i < rounds; ++i) {
        next = _mm256_loadu_ps(p);
        initialVal = _mm256_min_ps(initialVal, next);
        p += bitWidth;
      }

      // let sum up the final results
      const float* q = (const float*)&initialVal;

      v = TMIN(q[0], q[1]);
      v = TMIN(v, q[2]);
      v = TMIN(v, q[3]);
      v = TMIN(v, q[4]);
      v = TMIN(v, q[5]);
      v = TMIN(v, q[6]);
      v = TMIN(v, q[7]);

      // calculate the front and the reminder items in array list
      startElem += rounds * bitWidth;
      for (int32_t j = 0; j < remain; ++j) {
        if (v > p[j + startElem]) {
          v = p[j + startElem];
        }
      }
    }

    *val = v;
    numOfElems = numOfRows;
  }

  return numOfElems;
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

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc) {
  int32_t numOfElems = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pBuf = GET_ROWCELL_INTERBUF(pResInfo);
  pBuf->type = type;

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
        if (index >= 0) {
          pBuf->tuplePos = saveTupleData(pCtx, index, pCtx->pSrcBlock, NULL);
        }
      }
    } else {
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        int64_t prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        int64_t val = GET_INT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          *(int64_t*)&pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            if (index >= 0) {
              pBuf->tuplePos = saveTupleData(pCtx, index, pCtx->pSrcBlock, NULL);
            }
          }
        }
      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        uint64_t prev = 0;
        GET_TYPED_DATA(prev, uint64_t, type, &pBuf->v);

        uint64_t val = GET_UINT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          *(uint64_t*)&pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            if (index >= 0) {
              pBuf->tuplePos = saveTupleData(pCtx, index, pCtx->pSrcBlock, NULL);
            }
          }
        }
      } else if (type == TSDB_DATA_TYPE_DOUBLE) {
        double prev = 0;
        GET_TYPED_DATA(prev, double, type, &pBuf->v);

        double val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          *(double*)&pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            if (index >= 0) {
              pBuf->tuplePos = saveTupleData(pCtx, index, pCtx->pSrcBlock, NULL);
            }
          }
        }
      } else if (type == TSDB_DATA_TYPE_FLOAT) {
        float prev = 0;
        GET_TYPED_DATA(prev, float, type, &pBuf->v);

        float val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          *(float*)&pBuf->v = val;
        }

        if (pCtx->subsidiaries.num > 0) {
          index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
          if (index >= 0) {
            pBuf->tuplePos = saveTupleData(pCtx, index, pCtx->pSrcBlock, NULL);
          }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_INT) {
      int32_t* pData = (int32_t*)pCol->pData;
      int32_t* val = (int32_t*)&pBuf->v;

      numOfElems = handleInt32Col(pCol, start, numOfRows, pCtx, pBuf, isMinFunc);
#if 0
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          }
        }

        numOfElems += 1;
      }
#endif

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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
            pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          // NOTE: An faster version to avoid one additional comparison with FPU.
          if (isMinFunc) {  // min
            if (*val > pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
            }
          } else {  // max
            if (*val < pData[i]) {
              *val = pData[i];
              if (pCtx->subsidiaries.num > 0) {
                updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
              }
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
          pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
        }
        pBuf->assign = true;
      } else {
        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (isMinFunc) {  // min
          if (*val > pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        } else {  // max
          if (*val < pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }
      }

      numOfElems += 1;
    }
  } else if (type == TSDB_DATA_TYPE_FLOAT) {
    float* pData = (float*)pCol->pData;
    float* val = (float*)&pBuf->v;

    numOfElems = handleFloatCol(pCol, start, numOfRows, pCtx, pBuf, isMinFunc);
#if 0
    for (int32_t i = start; i < start + numOfRows; ++i) {
      if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      if (!pBuf->assign) {
        *val = pData[i];
        if (pCtx->subsidiaries.num > 0) {
          pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
        }
        pBuf->assign = true;
      } else {
#if 0
        if ((*val) == pData[i]) {
          continue;
        }

        if ((*val < pData[i]) ^ isMinFunc) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
#endif
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (isMinFunc) {  // min
          if (*val > pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        } else {  // max
          if (*val < pData[i]) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }
      }

      numOfElems += 1;
    }
#endif

  }

_min_max_over:
  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pBuf->nullTupleSaved) {
    pBuf->nullTuplePos = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, NULL);
    pBuf->nullTupleSaved = true;
  }
  return numOfElems;
}