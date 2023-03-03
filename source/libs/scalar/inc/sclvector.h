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

#ifndef _TD_COMMON_BIN_SCALAR_OPERATOR_H_
#define _TD_COMMON_BIN_SCALAR_OPERATOR_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSclVectorConvCtx {
  const SScalarParam* pIn;
  SScalarParam* pOut;
  int32_t startIndex; 
  int32_t endIndex;
  int16_t inType;
  int16_t outType;
} SSclVectorConvCtx;

typedef double (*_getDoubleValue_fn_t)(void *src, int32_t index);

static FORCE_INLINE double getVectorDoubleValue_TINYINT(void *src, int32_t index) {
  return (double)*((int8_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_UTINYINT(void *src, int32_t index) {
  return (double)*((uint8_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_SMALLINT(void *src, int32_t index) {
  return (double)*((int16_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_USMALLINT(void *src, int32_t index) {
  return (double)*((uint16_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_INT(void *src, int32_t index) {
  return (double)*((int32_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_UINT(void *src, int32_t index) {
  return (double)*((uint32_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_BIGINT(void *src, int32_t index) {
  return (double)*((int64_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_UBIGINT(void *src, int32_t index) {
  return (double)*((uint64_t *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_FLOAT(void *src, int32_t index) {
  return (double)*((float *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_DOUBLE(void *src, int32_t index) {
  return (double)*((double *)src + index);
}
static FORCE_INLINE double getVectorDoubleValue_BOOL(void *src, int32_t index) {
  return (double)*((bool *)src + index);
}

double getVectorDoubleValue_JSON(void *src, int32_t index);

static FORCE_INLINE _getDoubleValue_fn_t getVectorDoubleValueFn(int32_t srcType) {
  _getDoubleValue_fn_t p = NULL;
  if (srcType == TSDB_DATA_TYPE_TINYINT) {
    p = getVectorDoubleValue_TINYINT;
  } else if (srcType == TSDB_DATA_TYPE_UTINYINT) {
    p = getVectorDoubleValue_UTINYINT;
  } else if (srcType == TSDB_DATA_TYPE_SMALLINT) {
    p = getVectorDoubleValue_SMALLINT;
  } else if (srcType == TSDB_DATA_TYPE_USMALLINT) {
    p = getVectorDoubleValue_USMALLINT;
  } else if (srcType == TSDB_DATA_TYPE_INT) {
    p = getVectorDoubleValue_INT;
  } else if (srcType == TSDB_DATA_TYPE_UINT) {
    p = getVectorDoubleValue_UINT;
  } else if (srcType == TSDB_DATA_TYPE_BIGINT) {
    p = getVectorDoubleValue_BIGINT;
  } else if (srcType == TSDB_DATA_TYPE_UBIGINT) {
    p = getVectorDoubleValue_UBIGINT;
  } else if (srcType == TSDB_DATA_TYPE_FLOAT) {
    p = getVectorDoubleValue_FLOAT;
  } else if (srcType == TSDB_DATA_TYPE_DOUBLE) {
    p = getVectorDoubleValue_DOUBLE;
  } else if (srcType == TSDB_DATA_TYPE_TIMESTAMP) {
    p = getVectorDoubleValue_BIGINT;
  } else if (srcType == TSDB_DATA_TYPE_JSON) {
    p = getVectorDoubleValue_JSON;
  } else if (srcType == TSDB_DATA_TYPE_BOOL) {
    p = getVectorDoubleValue_BOOL;
  } else if (srcType == TSDB_DATA_TYPE_NULL) {
    p = NULL;
  } else {
    ASSERT(0);
  }
  return p;
}

typedef void (*_bufConverteFunc)(char *buf, SScalarParam *pOut, int32_t outType, int32_t *overflow);
typedef void (*_bin_scalar_fn_t)(SScalarParam *pLeft, SScalarParam *pRight, SScalarParam *output, int32_t order);
_bin_scalar_fn_t getBinScalarOperatorFn(int32_t binOperator);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_BIN_SCALAR_OPERATOR_H_*/
