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

static int32_t i32VectorCmpAVX2(const int32_t* pData, int32_t numOfRows, bool isMinFunc) {
  int32_t        v = 0;
  const int32_t  bitWidth = 256;
  const int32_t* p = pData;

  int32_t width = (bitWidth>>3u) / sizeof(int32_t);
  int32_t remain = numOfRows % width;
  int32_t rounds = numOfRows / width;

#if __AVX2__
  __m256i next;
  __m256i initialVal = _mm256_lddqu_si256((__m256i*)p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_max_epi32(initialVal, next);
      p += width;
    }

    // let compare  the final results
    const int32_t* q = (const int32_t*)&initialVal;
    v = TMAX(q[0], q[1]);
    for (int32_t k = 1; k < width; ++k) {
      v = TMAX(v, q[k]);
    }

    // calculate the front and the reminder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v < p[j + start]) {
        v = p[j + start];
      }
    }
  } else {  // min function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_min_epi32(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const int32_t* q = (const int32_t*)&initialVal;
    v = TMIN(q[0], q[1]);
    for (int32_t k = 1; k < width; ++k) {
      v = TMIN(v, q[k]);
    }

    // calculate the front and the remainder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v > p[j + start]) {
        v = p[j + start];
      }
    }
  }
#endif

  return v;
}

static float floatVectorCmpAVX(const float* pData, int32_t numOfRows, bool isMinFunc) {
  float v = 0;
  const int32_t bitWidth = 256;
  const float* p = pData;

  int32_t width = (bitWidth>>3u) / sizeof(float);
  int32_t remain = numOfRows % width;
  int32_t rounds = numOfRows / width;

#if __AVX__

  __m256 next;
  __m256 initialVal = _mm256_loadu_ps(p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_ps(p);
      initialVal = _mm256_max_ps(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const float* q = (const float*)&initialVal;
    v = TMAX(q[0], q[1]);
    for (int32_t k = 1; k < width; ++k) {
      v = TMAX(v, q[k]);
    }

    // calculate the front and the reminder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v < p[j + width]) {
        v = p[j + width];
      }
    }
  } else {  // min function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_ps(p);
      initialVal = _mm256_min_ps(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const float* q = (const float*)&initialVal;
    v = TMIN(q[0], q[1]);
    for (int32_t k = 1; k < width; ++k) {
      v = TMIN(v, q[k]);
    }

    // calculate the front and the reminder items in array list
    int32_t start = rounds * bitWidth;
    for (int32_t j = 0; j < remain; ++j) {
      if (v > p[j + start]) {
        v = p[j + start];
      }
    }
  }
#endif

  return v;
}

static int8_t i8VectorCmpAVX2(const int8_t* pData, int32_t numOfRows, bool isMinFunc) {
  int8_t        v = 0;
  const int32_t  bitWidth = 256;
  const int8_t* p = pData;

  int32_t width = (bitWidth>>3u) / sizeof(int8_t);
  int32_t remain = numOfRows % width;
  int32_t rounds = numOfRows / width;

#if __AVX2__
  __m256i next;
  __m256i initialVal = _mm256_lddqu_si256((__m256i*)p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_max_epi8(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const int8_t* q = (const int8_t*)&initialVal;
    v = TMAX(q[0], q[1]);
    for (int32_t k = 1; k < width; ++k) {
      v = TMAX(v, q[k]);
    }

    // calculate the front and the reminder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v < p[j + start]) {
        v = p[j + start];
      }
    }
  } else {  // min function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_min_epi8(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const int8_t* q = (const int8_t*)&initialVal;

    v = TMIN(q[0], q[1]);
    for(int32_t k = 1; k < width; ++k) {
      v = TMIN(v, q[k]);
    }

    // calculate the front and the remainder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v > p[j + start]) {
        v = p[j + start];
      }
    }
  }
#endif

  return v;
}

static int16_t i16VectorCmpAVX2(const int16_t* pData, int32_t numOfRows, bool isMinFunc) {
  int16_t        v = 0;
  const int32_t  bitWidth = 256;
  const int16_t* p = pData;

  int32_t width = (bitWidth>>3u) / sizeof(int16_t);
  int32_t remain = numOfRows % width;
  int32_t rounds = numOfRows / width;

#if __AVX2__
  __m256i next;
  __m256i initialVal = _mm256_lddqu_si256((__m256i*)p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_max_epi16(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const int16_t* q = (const int16_t*)&initialVal;

    v = TMAX(q[0], q[1]);
    for(int32_t k = 1; k < width; ++k) {
      v = TMAX(v, q[k]);
    }

    // calculate the front and the reminder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v < p[j + start]) {
        v = p[j + start];
      }
    }
  } else {  // min function
    for (int32_t i = 0; i < rounds; ++i) {
      next = _mm256_lddqu_si256((__m256i*)p);
      initialVal = _mm256_min_epi16(initialVal, next);
      p += width;
    }

    // let sum up the final results
    const int16_t* q = (const int16_t*)&initialVal;

    v = TMIN(q[0], q[1]);
    for(int32_t k = 1; k < width; ++k) {
      v = TMIN(v, q[k]);
    }

    // calculate the front and the remainder items in array list
    int32_t start = rounds * width;
    for (int32_t j = 0; j < remain; ++j) {
      if (v > p[j + start]) {
        v = p[j + start];
      }
    }
  }
#endif

  return v;
}

static int32_t handleInt8Col(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                             SMinmaxResInfo* pBuf, bool isMinFunc) {
  int8_t* pData = (int8_t*)pCol->pData;
  int8_t* val = (int8_t*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows <= 8 || pCtx->subsidiaries.num > 0) {
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    }
  } else {  // not has null value
    // AVX2 version to speedup the loop
    if (tsAVX2Enable && tsSIMDEnable) {
      *val = i8VectorCmpAVX2(pData, numOfRows, isMinFunc);
    } else {
      if (!pBuf->assign) {
        *val = pData[0];
        pBuf->assign = true;
      }

      if (isMinFunc) {  // min
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val > pData[i]) {
            *val = pData[i];
          }
        }
      } else {  // max
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val < pData[i]) {
            *val = pData[i];
          }
        }
      }
    }

    numOfElems = numOfRows;
  }

  return numOfElems;
}

static int32_t handleInt16Col(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                             SMinmaxResInfo* pBuf, bool isMinFunc) {
  int16_t* pData = (int16_t*)pCol->pData;
  int16_t* val = (int16_t*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows <= 8 || pCtx->subsidiaries.num > 0) {
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    }
  } else {  // not has null value
    // AVX2 version to speedup the loop
    if (tsAVX2Enable && tsSIMDEnable) {
      *val = i16VectorCmpAVX2(pData, numOfRows, isMinFunc);
    } else {
      if (!pBuf->assign) {
        *val = pData[0];
        pBuf->assign = true;
      }

      if (isMinFunc) {  // min
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val > pData[i]) {
            *val = pData[i];
          }
        }
      } else {  // max
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val < pData[i]) {
            *val = pData[i];
          }
        }
      }
    }

    numOfElems = numOfRows;
  }

  return numOfElems;
}

static int32_t handleInt32Col(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                              SMinmaxResInfo* pBuf, bool isMinFunc) {
  int32_t* pData = (int32_t*)pCol->pData;
  int32_t* val = (int32_t*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows <= 8 || pCtx->subsidiaries.num > 0) {
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    }
  } else {  // not has null value
    // AVX2 version to speedup the loop
    if (tsAVX2Enable && tsSIMDEnable) {
      *val = i32VectorCmpAVX2(pData, numOfRows, isMinFunc);
    } else {
      if (!pBuf->assign) {
        *val = pData[0];
        pBuf->assign = true;
      }

      if (isMinFunc) {  // min
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val > pData[i]) {
            *val = pData[i];
          }
        }
      } else {  // max
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val < pData[i]) {
            *val = pData[i];
          }
        }
      }
    }

    numOfElems = numOfRows;
  }

  return numOfElems;
}

static int32_t handleInt64Col(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                              SMinmaxResInfo* pBuf, bool isMinFunc) {
  int32_t* pData = (int32_t*)pCol->pData;
  int32_t* val = (int32_t*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || pCtx->subsidiaries.num > 0) {
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }

    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }
    }
  } else {  // not has null value
            // AVX2 version to speedup the loop
    if (!pBuf->assign) {
      *val = pData[0];
      pBuf->assign = true;
    }

    if (isMinFunc) {  // min
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (*val > pData[i]) {
          *val = pData[i];
        }
      }
    } else {  // max
      for (int32_t i = start; i < start + numOfRows; ++i) {
        if (*val < pData[i]) {
          *val = pData[i];
        }
      }
    }

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
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }
    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }
    }
  } else {  // not has null value
    // AVX version to speedup the loop
    if (tsAVXEnable && tsSIMDEnable) {
      *val = (double) floatVectorCmpAVX(pData, numOfRows, isMinFunc);
    } else {
      if (!pBuf->assign) {
        *val = pData[0];
        pBuf->assign = true;
      }

      if (isMinFunc) {  // min
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val > pData[i]) {
            *val = pData[i];
          }
        }
      } else {  // max
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val < pData[i]) {
            *val = pData[i];
          }
        }
      }
    }

    numOfElems = numOfRows;
  }

  return numOfElems;
}

static int32_t handleDoubleCol(SColumnInfoData* pCol, int32_t start, int32_t numOfRows, SqlFunctionCtx* pCtx,
                              SMinmaxResInfo* pBuf, bool isMinFunc) {
  float* pData = (float*)pCol->pData;
  double* val = (double*)&pBuf->v;

  int32_t numOfElems = 0;
  if (pCol->hasNull || numOfRows < 8 || pCtx->subsidiaries.num > 0) {
    int32_t i = start;
    while (i < (start + numOfRows)) {
      if (!colDataIsNull_f(pCol->nullbitmap, i)) {
        break;
      }
      i += 1;
    }

    if ((i < (start + numOfRows)) && (!pBuf->assign)) {
      *val = pData[i];
      if (pCtx->subsidiaries.num > 0) {
        pBuf->tuplePos = saveTupleData(pCtx, i, pCtx->pSrcBlock, NULL);
      }
      pBuf->assign = true;
      numOfElems += 1;
    }

    if (isMinFunc) {  // min
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (*val > pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }
    } else {  // max function
      for (; i < start + numOfRows; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        // ignore the equivalent data value
        // NOTE: An faster version to avoid one additional comparison with FPU.
        if (*val < pData[i]) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            updateTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
        numOfElems += 1;
      }
    }
  } else {  // not has null value
    // AVX version to speedup the loop
    if (tsAVXEnable && tsSIMDEnable) {
      *val = (double) floatVectorCmpAVX(pData, numOfRows, isMinFunc);
    } else {
      if (!pBuf->assign) {
        *val = pData[0];
        pBuf->assign = true;
      }

      if (isMinFunc) {  // min
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val > pData[i]) {
            *val = pData[i];
          }
        }
      } else {  // max
        for (int32_t i = start; i < start + numOfRows; ++i) {
          if (*val < pData[i]) {
            *val = pData[i];
          }
        }
      }
    }

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
  if (pInput->colDataSMAIsSet) {
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
      numOfElems = handleInt8Col(pCol, start, numOfRows, pCtx, pBuf, isMinFunc);
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      numOfElems = handleInt16Col(pCol, start, numOfRows, pCtx, pBuf, isMinFunc);
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