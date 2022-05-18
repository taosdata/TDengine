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

#ifndef _TD_COMMON_TTYPE_H_
#define _TD_COMMON_TTYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"
#include "types.h"

// ----------------- For variable data types such as TSDB_DATA_TYPE_BINARY and TSDB_DATA_TYPE_NCHAR
typedef uint32_t TDRowLenT;
typedef uint8_t  TDRowValT;
typedef uint64_t TDRowVerT;
typedef int16_t  col_id_t;
typedef int8_t   col_type_t;
typedef int32_t  col_bytes_t;
typedef uint16_t schema_ver_t;
typedef int32_t  func_id_t;

#pragma pack(push, 1)
typedef struct {
  VarDataLenT len;
  uint8_t     data;
} SBinaryNullT;

typedef struct {
  VarDataLenT len;
  uint32_t    data;
} SNCharNullT;
#pragma pack(pop)

#define varDataTLen(v)         (sizeof(VarDataLenT) + varDataLen(v))
#define varDataCopy(dst, v)    memcpy((dst), (void *)(v), varDataTLen(v))
#define varDataLenByData(v)    (*(VarDataLenT *)(((char *)(v)) - VARSTR_HEADER_SIZE))
#define varDataSetLen(v, _len) (((VarDataLenT *)(v))[0] = (VarDataLenT)(_len))
#define IS_VAR_DATA_TYPE(t)    (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_NCHAR) || ((t) == TSDB_DATA_TYPE_JSON))
#define IS_STR_DATA_TYPE(t)    (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_NCHAR))

#define varDataNetLen(v)  (htons(((VarDataLenT *)(v))[0]))
#define varDataNetTLen(v) (sizeof(VarDataLenT) + varDataNetLen(v))

// this data type is internally used only in 'in' query to hold the values
#define TSDB_DATA_TYPE_POINTER_ARRAY (1000)
#define TSDB_DATA_TYPE_VALUE_ARRAY   (1001)

#define GET_TYPED_DATA(_v, _finalType, _type, _data) \
  do {                                               \
    switch (_type) {                                 \
      case TSDB_DATA_TYPE_BOOL:                      \
      case TSDB_DATA_TYPE_TINYINT:                   \
        (_v) = (_finalType)GET_INT8_VAL(_data);      \
        break;                                       \
      case TSDB_DATA_TYPE_UTINYINT:                  \
        (_v) = (_finalType)GET_UINT8_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_SMALLINT:                  \
        (_v) = (_finalType)GET_INT16_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_USMALLINT:                 \
        (_v) = (_finalType)GET_UINT16_VAL(_data);    \
        break;                                       \
      case TSDB_DATA_TYPE_TIMESTAMP:                 \
      case TSDB_DATA_TYPE_BIGINT:                    \
        (_v) = (_finalType)(GET_INT64_VAL(_data));   \
        break;                                       \
      case TSDB_DATA_TYPE_UBIGINT:                   \
        (_v) = (_finalType)(GET_UINT64_VAL(_data));  \
        break;                                       \
      case TSDB_DATA_TYPE_FLOAT:                     \
        (_v) = (_finalType)GET_FLOAT_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_DOUBLE:                    \
        (_v) = (_finalType)GET_DOUBLE_VAL(_data);    \
        break;                                       \
      case TSDB_DATA_TYPE_UINT:                      \
        (_v) = (_finalType)GET_UINT32_VAL(_data);    \
        break;                                       \
      case TSDB_DATA_TYPE_INT:                       \
        (_v) = (_finalType)GET_INT32_VAL(_data);     \
        break;                                       \
      default:                                       \
        (_v) = (_finalType)varDataLen(_data);        \
        break;                                       \
    }                                                \
  } while (0)

#define SET_TYPED_DATA(_v, _type, _data)       \
  do {                                         \
    switch (_type) {                           \
      case TSDB_DATA_TYPE_BOOL:                \
        *(bool *)(_v) = (bool)(_data);         \
        break;                                 \
      case TSDB_DATA_TYPE_TINYINT:             \
        *(int8_t *)(_v) = (int8_t)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UTINYINT:            \
        *(uint8_t *)(_v) = (uint8_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_SMALLINT:            \
        *(int16_t *)(_v) = (int16_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_USMALLINT:           \
        *(uint16_t *)(_v) = (uint16_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_BIGINT:              \
      case TSDB_DATA_TYPE_TIMESTAMP:           \
        *(int64_t *)(_v) = (int64_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_UBIGINT:             \
        *(uint64_t *)(_v) = (uint64_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_FLOAT:               \
        *(float *)(_v) = (float)(_data);       \
        break;                                 \
      case TSDB_DATA_TYPE_DOUBLE:              \
        *(double *)(_v) = (double)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UINT:                \
        *(uint32_t *)(_v) = (uint32_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_INT:                 \
        *(int32_t *)(_v) = (int32_t)(_data);   \
        break;                                 \
      default:                                 \
        break;                                 \
    }                                          \
  } while (0)

#define NUM_TO_STRING(_inputType, _input, _outputBytes, _output)     \
  do {                                         \
    switch (_inputType) {                      \
      case TSDB_DATA_TYPE_TINYINT:             \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int8_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_UTINYINT:            \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint8_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_SMALLINT:            \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int16_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_USMALLINT:           \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint16_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_TIMESTAMP:           \
      case TSDB_DATA_TYPE_BIGINT:              \
        snprintf(_output, (int32_t)(_outputBytes), "%" PRId64, *(int64_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_UBIGINT:             \
        snprintf(_output, (int32_t)(_outputBytes), "%" PRIu64, *(uint64_t *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_FLOAT:               \
        snprintf(_output, (int32_t)(_outputBytes), "%f", *(float *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_DOUBLE:              \
        snprintf(_output, (int32_t)(_outputBytes), "%f", *(double *)(_input));     \
        break;                                 \
      case TSDB_DATA_TYPE_UINT:                \
        snprintf(_output, (int32_t)(_outputBytes), "%u", *(uint32_t *)(_input));     \
        break;                                 \
      default:                                 \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int32_t *)(_input));     \
        break;                                 \
    }                                          \
  } while (0)

//TODO: use varchar(0) to represent NULL type
#define IS_NULL_TYPE(_t)             ((_t) == TSDB_DATA_TYPE_NULL)
#define IS_SIGNED_NUMERIC_TYPE(_t)   ((_t) >= TSDB_DATA_TYPE_TINYINT && (_t) <= TSDB_DATA_TYPE_BIGINT)
#define IS_UNSIGNED_NUMERIC_TYPE(_t) ((_t) >= TSDB_DATA_TYPE_UTINYINT && (_t) <= TSDB_DATA_TYPE_UBIGINT)
#define IS_FLOAT_TYPE(_t)            ((_t) == TSDB_DATA_TYPE_FLOAT || (_t) == TSDB_DATA_TYPE_DOUBLE)
#define IS_INTEGER_TYPE(_t)          ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)))

#define IS_NUMERIC_TYPE(_t) ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)) || (IS_FLOAT_TYPE(_t)))
#define IS_MATHABLE_TYPE(_t) (IS_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL) || (_t) == (TSDB_DATA_TYPE_TIMESTAMP))

#define IS_VALID_TINYINT(_t)   ((_t) >= INT8_MIN && (_t) <= INT8_MAX)
#define IS_VALID_SMALLINT(_t)  ((_t) >= INT16_MIN && (_t) <= INT16_MAX)
#define IS_VALID_INT(_t)       ((_t) >= INT32_MIN && (_t) <= INT32_MAX)
#define IS_VALID_BIGINT(_t)    ((_t) >= INT64_MIN && (_t) <= INT64_MAX)
#define IS_VALID_UTINYINT(_t)  ((_t) >= 0 && (_t) <= UINT8_MAX)
#define IS_VALID_USMALLINT(_t) ((_t) >= 0 && (_t) <= UINT16_MAX)
#define IS_VALID_UINT(_t)      ((_t) >= 0 && (_t) <= UINT32_MAX)
#define IS_VALID_UBIGINT(_t)   ((_t) >= 0 && (_t) <= UINT64_MAX)
#define IS_VALID_FLOAT(_t)     ((_t) >= -FLT_MAX && (_t) <= FLT_MAX)
#define IS_VALID_DOUBLE(_t)    ((_t) >= -DBL_MAX && (_t) <= DBL_MAX)

#define IS_CONVERT_AS_SIGNED(_t) \
  (IS_SIGNED_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL) || (_t) == (TSDB_DATA_TYPE_TIMESTAMP))
#define IS_CONVERT_AS_UNSIGNED(_t) (IS_UNSIGNED_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL))

// TODO remove this function
static FORCE_INLINE bool isNull(const void *val, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return *(uint8_t *)val == TSDB_DATA_BOOL_NULL;
    case TSDB_DATA_TYPE_TINYINT:
      return *(uint8_t *)val == TSDB_DATA_TINYINT_NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      return *(uint16_t *)val == TSDB_DATA_SMALLINT_NULL;
    case TSDB_DATA_TYPE_INT:
      return *(uint32_t *)val == TSDB_DATA_INT_NULL;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return *(uint64_t *)val == TSDB_DATA_BIGINT_NULL;
    case TSDB_DATA_TYPE_FLOAT:
      return *(uint32_t *)val == TSDB_DATA_FLOAT_NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      return *(uint64_t *)val == TSDB_DATA_DOUBLE_NULL;
    case TSDB_DATA_TYPE_NCHAR:
      return varDataLen(val) == sizeof(int32_t) && *(uint32_t *)varDataVal(val) == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return varDataLen(val) == sizeof(int8_t) && *(uint8_t *)varDataVal(val) == TSDB_DATA_BINARY_NULL;
    case TSDB_DATA_TYPE_UTINYINT:
      return *(uint8_t *)val == TSDB_DATA_UTINYINT_NULL;
    case TSDB_DATA_TYPE_USMALLINT:
      return *(uint16_t *)val == TSDB_DATA_USMALLINT_NULL;
    case TSDB_DATA_TYPE_UINT:
      return *(uint32_t *)val == TSDB_DATA_UINT_NULL;
    case TSDB_DATA_TYPE_UBIGINT:
      return *(uint64_t *)val == TSDB_DATA_UBIGINT_NULL;

    default:
      return false;
  };
}

typedef struct tDataTypeDescriptor {
  int16_t type;
  int16_t nameLen;
  int32_t bytes;
  char   *name;
  int64_t minValue;
  int64_t maxValue;
  int32_t (*compFunc)(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                      int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
  int32_t (*decompFunc)(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                        int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
  void (*statisFunc)(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numofrow, int64_t *min,
                     int64_t *max, int64_t *sum, int16_t *minindex, int16_t *maxindex, int16_t *numofnull);
} tDataTypeDescriptor;

extern tDataTypeDescriptor tDataTypes[TSDB_DATA_TYPE_MAX];

bool isValidDataType(int32_t type);

void        setVardataNull(void *val, int32_t type);
void        setNull(void *val, int32_t type, int32_t bytes);
void        setNullN(void *val, int32_t type, int32_t bytes, int32_t numOfElems);
const void *getNullValue(int32_t type);

void  assignVal(char *val, const char *src, int32_t len, int32_t type);
void  tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size, void *buf);
void  operateVal(void *dst, void *s1, void *s2, int32_t optr, int32_t type);
void *getDataMin(int32_t type);
void *getDataMax(int32_t type);

#define SET_DOUBLE_NULL(v) (*(uint64_t *)(v) = TSDB_DATA_DOUBLE_NULL)
#define SET_BIGINT_NULL(v) (*(uint64_t *)(v) = TSDB_DATA_BIGINT_NULL)

#ifdef __cplusplus
}
#endif

#endif  /*_TD_COMMON_TTYPE_H_*/
