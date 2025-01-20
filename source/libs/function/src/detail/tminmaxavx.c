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

#ifdef __AVX2__
static void calculateRounds(int32_t numOfRows, int32_t bytes, int32_t* remainder, int32_t* rounds, int32_t* width) {
  const int32_t bitWidth = 256;

  *width = (bitWidth >> 3u) / bytes;
  *remainder = numOfRows % (*width);
  *rounds = numOfRows / (*width);
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

#define EXTRACT_MAX_VAL(_first, _sec, _width, _remain, _v) \
  __COMPARE_EXTRACT_MAX(0, (_width), (_v), (_first))       \
  __COMPARE_EXTRACT_MAX(0, (_remain), (_v), (_sec))

#define EXTRACT_MIN_VAL(_first, _sec, _width, _remain, _v) \
  __COMPARE_EXTRACT_MIN(0, (_width), (_v), (_first))       \
  __COMPARE_EXTRACT_MIN(0, (_remain), (_v), (_sec))

#define CMP_TYPE_MIN_MAX(type, cmp)                      \
  const type* p = pData;                                 \
  __m256i     initVal = _mm256_lddqu_si256((__m256i*)p); \
  p += width;                                            \
  for (int32_t i = 1; i < (rounds); ++i) {               \
    __m256i next = _mm256_lddqu_si256((__m256i*)p);      \
    initVal = CMP_FUNC_##cmp##_##type(initVal, next);    \
    p += width;                                          \
  }                                                      \
  const type* q = (const type*)&initVal;                 \
  type*       v = (type*)res;                            \
  EXTRACT_##cmp##_VAL(q, p, width, remain, *v)
#endif

int32_t i8VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res) {
#ifdef __AVX2__
  const int8_t* p = pData;

  int32_t width, remain, rounds;
  calculateRounds(numOfRows, sizeof(int8_t), &remain, &rounds, &width);

#define CMP_FUNC_MIN_int8_t  _mm256_min_epi8
#define CMP_FUNC_MAX_int8_t  _mm256_max_epi8
#define CMP_FUNC_MIN_uint8_t _mm256_min_epu8
#define CMP_FUNC_MAX_uint8_t _mm256_max_epu8

  if (!isMinFunc) {  // max function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int8_t, MAX);
    } else {
      CMP_TYPE_MIN_MAX(uint8_t, MAX);
    }
  } else {  // min function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int8_t, MIN);
    } else {
      CMP_TYPE_MIN_MAX(uint8_t, MIN);
    }
  }
  return TSDB_CODE_SUCCESS;
#else
  uError("unable run %s without avx2 instructions", __func__);
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int32_t i16VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res) {
#ifdef __AVX2__
  int32_t width, remain, rounds;
  calculateRounds(numOfRows, sizeof(int16_t), &remain, &rounds, &width);

#define CMP_FUNC_MIN_int16_t  _mm256_min_epi16
#define CMP_FUNC_MAX_int16_t  _mm256_max_epi16
#define CMP_FUNC_MIN_uint16_t _mm256_min_epu16
#define CMP_FUNC_MAX_uint16_t _mm256_max_epu16
  if (!isMinFunc) {  // max function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int16_t, MAX);
    } else {
      CMP_TYPE_MIN_MAX(uint16_t, MAX);
    }
  } else {  // min function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int16_t, MIN);
    } else {
      CMP_TYPE_MIN_MAX(uint16_t, MIN);
    }
  }
  return TSDB_CODE_SUCCESS;
#else
  uError("unable run %s without avx2 instructions", __func__);
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int32_t i32VectorCmpAVX2(const void* pData, int32_t numOfRows, bool isMinFunc, bool signVal, int64_t* res) {
#ifdef __AVX2__
  int32_t width, remain, rounds;
  calculateRounds(numOfRows, sizeof(int32_t), &remain, &rounds, &width);

#define CMP_FUNC_MIN_int32_t  _mm256_min_epi32
#define CMP_FUNC_MAX_int32_t  _mm256_max_epi32
#define CMP_FUNC_MIN_uint32_t _mm256_min_epu32
#define CMP_FUNC_MAX_uint32_t _mm256_max_epu32
  if (!isMinFunc) {  // max function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int32_t, MAX);
    } else {
      CMP_TYPE_MIN_MAX(uint32_t, MAX);
    }
  } else {  // min function
    if (signVal) {
      CMP_TYPE_MIN_MAX(int32_t, MIN);
    } else {
      CMP_TYPE_MIN_MAX(uint32_t, MIN);
    }
  }
  return TSDB_CODE_SUCCESS;
#else
  uError("unable run %s without avx2 instructions", __func__);
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int32_t floatVectorCmpAVX2(const float* pData, int32_t numOfRows, bool isMinFunc, float* res) {
#ifdef __AVX2__
  const float* p = pData;

  int32_t width, remain, rounds;
  calculateRounds(numOfRows, sizeof(float), &remain, &rounds, &width);

  __m256 next;
  __m256 initVal = _mm256_loadu_ps(p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_ps(p);
      initVal = _mm256_max_ps(initVal, next);
      p += width;
    }

    const float* q = (const float*)&initVal;
    EXTRACT_MAX_VAL(q, p, width, remain, *res)
  } else {  // min function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_ps(p);
      initVal = _mm256_min_ps(initVal, next);
      p += width;
    }

    const float* q = (const float*)&initVal;
    EXTRACT_MIN_VAL(q, p, width, remain, *res)
  }
  return TSDB_CODE_SUCCESS;
#else
  uError("unable run %s without avx2 instructions", __func__);
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int32_t doubleVectorCmpAVX2(const double* pData, int32_t numOfRows, bool isMinFunc, double* res) {
#ifdef __AVX2__
  const double* p = pData;

  int32_t width, remain, rounds;
  calculateRounds(numOfRows, sizeof(double), &remain, &rounds, &width);

  __m256d next;
  __m256d initVal = _mm256_loadu_pd(p);
  p += width;

  if (!isMinFunc) {  // max function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_pd(p);
      initVal = _mm256_max_pd(initVal, next);
      p += width;
    }

    // let sum up the final results
    const double* q = (const double*)&initVal;
    EXTRACT_MAX_VAL(q, p, width, remain, *res)
  } else {  // min function
    for (int32_t i = 1; i < rounds; ++i) {
      next = _mm256_loadu_pd(p);
      initVal = _mm256_min_pd(initVal, next);
      p += width;
    }

    // let sum up the final results
    const double* q = (const double*)&initVal;
    EXTRACT_MIN_VAL(q, p, width, remain, *res)
  }
  return TSDB_CODE_SUCCESS;
#else
  uError("unable run %s without avx2 instructions", __func__);
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}
