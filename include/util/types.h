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

#ifndef _TD_TYPES_H_
#define _TD_TYPES_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define GET_INT8_VAL(x)   (*(int8_t *)(x))
#define GET_INT16_VAL(x)  (*(int16_t *)(x))
#define GET_INT32_VAL(x)  (*(int32_t *)(x))
#define GET_INT64_VAL(x)  (*(int64_t *)(x))
#define GET_UINT8_VAL(x)  (*(uint8_t *)(x))
#define GET_UINT16_VAL(x) (*(uint16_t *)(x))
#define GET_UINT32_VAL(x) (*(uint32_t *)(x))
#define GET_UINT64_VAL(x) (*(uint64_t *)(x))

static FORCE_INLINE float taos_align_get_float(const char *pBuf) {
#if __STDC_VERSION__ >= 201112LL
  static_assert(sizeof(float) == sizeof(uint32_t), "sizeof(float) must equal to sizeof(uint32_t)");
#else
  assert(sizeof(float) == sizeof(uint32_t));
#endif
  float fv = 0;
  memcpy(&fv, pBuf, sizeof(fv));  // in ARM, return *((const float*)(pBuf)) may cause problem
  return fv;
}

static FORCE_INLINE double taos_align_get_double(const char *pBuf) {
#if __STDC_VERSION__ >= 201112LL
  static_assert(sizeof(double) == sizeof(uint64_t), "sizeof(double) must equal to sizeof(uint64_t)");
#else
  assert(sizeof(double) == sizeof(uint64_t));
#endif
  double dv = 0;
  memcpy(&dv, pBuf, sizeof(dv));  // in ARM, return *((const double*)(pBuf)) may cause problem
  return dv;
}

// #ifdef _TD_ARM_32
//   float  taos_align_get_float(const char* pBuf);
//   double taos_align_get_double(const char* pBuf);

//   #define GET_FLOAT_VAL(x)       taos_align_get_float(x)
//   #define GET_DOUBLE_VAL(x)      taos_align_get_double(x)
//   #define SET_FLOAT_VAL(x, y)  { float z = (float)(y);   (*(int32_t*) x = *(int32_t*)(&z)); }
//   #define SET_DOUBLE_VAL(x, y) { double z = (double)(y); (*(int64_t*) x = *(int64_t*)(&z)); }
//   #define SET_FLOAT_PTR(x, y)  { (*(int32_t*) x = *(int32_t*)y); }
//   #define SET_DOUBLE_PTR(x, y) { (*(int64_t*) x = *(int64_t*)y); }
// #else
#define GET_FLOAT_VAL(x)  (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))
#define SET_BIGINT_VAL(x, y) \
  { (*(int64_t *)(x)) = (int64_t)(y); }
#define SET_FLOAT_VAL(x, y) \
  { (*(float *)(x)) = (float)(y); }
#define SET_DOUBLE_VAL(x, y) \
  { (*(double *)(x)) = (double)(y); }
#define SET_FLOAT_PTR(x, y) \
  { (*(float *)(x)) = (*(float *)(y)); }
#define SET_DOUBLE_PTR(x, y) \
  { (*(double *)(x)) = (*(double *)(y)); }
// #endif

typedef uint16_t VarDataLenT;  // maxVarDataLen: 65535
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define varDataLen(v)  ((VarDataLenT *)(v))[0]
#define varDataVal(v)  ((char *)(v) + VARSTR_HEADER_SIZE)
#define varDataTLen(v) (sizeof(VarDataLenT) + varDataLen(v))

typedef int32_t VarDataOffsetT;

typedef struct tstr {
  VarDataLenT len;
  char        data[];
} tstr;

#ifdef __cplusplus
}
#endif

#endif /*_TD_TYPES_H_*/
