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

#define _DEFAULT_SOURCE
#include "ttypes.h"
#include "tcompression.h"
#include "trow.h"

const int32_t TYPE_BYTES[16] = {
    -1,                      // TSDB_DATA_TYPE_NULL
    CHAR_BYTES,              // TSDB_DATA_TYPE_BOOL
    CHAR_BYTES,              // TSDB_DATA_TYPE_TINYINT
    SHORT_BYTES,             // TSDB_DATA_TYPE_SMALLINT
    INT_BYTES,               // TSDB_DATA_TYPE_INT
    sizeof(int64_t),         // TSDB_DATA_TYPE_BIGINT
    FLOAT_BYTES,             // TSDB_DATA_TYPE_FLOAT
    DOUBLE_BYTES,            // TSDB_DATA_TYPE_DOUBLE
    sizeof(VarDataOffsetT),  // TSDB_DATA_TYPE_BINARY
    sizeof(TSKEY),           // TSDB_DATA_TYPE_TIMESTAMP
    sizeof(VarDataOffsetT),  // TSDB_DATA_TYPE_NCHAR
    CHAR_BYTES,              // TSDB_DATA_TYPE_UTINYINT
    SHORT_BYTES,             // TSDB_DATA_TYPE_USMALLINT
    INT_BYTES,               // TSDB_DATA_TYPE_UINT
    sizeof(uint64_t),        // TSDB_DATA_TYPE_UBIGINT
    TSDB_MAX_JSON_TAG_LEN,   // TSDB_DATA_TYPE_JSON
};

#define DO_STATICS(__sum, __min, __max, __minIndex, __maxIndex, _list, _index) \
  do {                                                                         \
    (__sum) += (_list)[(_index)];                                              \
    if ((__min) > (_list)[(_index)]) {                                         \
      (__min) = (_list)[(_index)];                                             \
      (__minIndex) = (_index);                                                 \
    }                                                                          \
                                                                               \
    if ((__max) < (_list)[(_index)]) {                                         \
      (__max) = (_list)[(_index)];                                             \
      (__maxIndex) = (_index);                                                 \
    }                                                                          \
  } while (0)

static void getStatics_bool(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                            int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int8_t *data = (int8_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (data[i] == TSDB_DATA_BOOL_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(*sum, *min, *max, *minIndex, *maxIndex, data, i);
  }
}

static void getStatics_i8(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                          int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int8_t *data = (int8_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint8_t)data[i]) == TSDB_DATA_TINYINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(*sum, *min, *max, *minIndex, *maxIndex, data, i);
  }
}

static void getStatics_u8(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                          int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  uint8_t *data = (uint8_t *)pData;
  uint64_t _min = UINT64_MAX;
  uint64_t _max = 0;
  uint64_t _sum = 0;

  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint8_t)data[i]) == TSDB_DATA_UTINYINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(_sum, _min, _max, *minIndex, *maxIndex, data, i);
  }

  *min = _min;
  *max = _max;
  *sum = _sum;
}

static void getStatics_i16(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int16_t *data = (int16_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint16_t)data[i]) == TSDB_DATA_SMALLINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(*sum, *min, *max, *minIndex, *maxIndex, data, i);
  }
}

static void getStatics_u16(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  uint16_t *data = (uint16_t *)pData;
  uint64_t  _min = UINT64_MAX;
  uint64_t  _max = 0;
  uint64_t  _sum = 0;

  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint16_t)data[i]) == TSDB_DATA_USMALLINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(_sum, _min, _max, *minIndex, *maxIndex, data, i);
  }

  *min = _min;
  *max = _max;
  *sum = _sum;
}

static void getStatics_i32(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int32_t *data = (int32_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint32_t)data[i]) == TSDB_DATA_INT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(*sum, *min, *max, *minIndex, *maxIndex, data, i);
  }
}

static void getStatics_u32(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  uint32_t *data = (uint32_t *)pData;
  uint64_t  _min = UINT64_MAX;
  uint64_t  _max = 0;
  uint64_t  _sum = 0;

  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint32_t)data[i]) == TSDB_DATA_UINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(_sum, _min, _max, *minIndex, *maxIndex, data, i);
  }

  *min = _min;
  *max = _max;
  *sum = _sum;
}

static void getStatics_i64(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int64_t *data = (int64_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint64_t)data[i]) == TSDB_DATA_BIGINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(*sum, *min, *max, *minIndex, *maxIndex, data, i);
  }
}

static void getStatics_u64(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  uint64_t *data = (uint64_t *)pData;
  uint64_t  _min = UINT64_MAX;
  uint64_t  _max = 0;
  uint64_t  _sum = 0;

  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (((uint64_t)data[i]) == TSDB_DATA_UBIGINT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    DO_STATICS(_sum, _min, _max, *minIndex, *maxIndex, data, i);
  }

  *min = _min;
  *max = _max;
  *sum = _sum;
}

static void getStatics_f(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                         int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  float *data = (float *)pData;
  float  fmin = FLT_MAX;
  float  fmax = -FLT_MAX;
  double dsum = 0;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if ((*(uint32_t *)&(data[i])) == TSDB_DATA_FLOAT_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    float fv = GET_FLOAT_VAL((const char *)&(data[i]));

    dsum += fv;
    if (fmin > fv) {
      fmin = fv;
      *minIndex = i;
    }

    if (fmax < fv) {
      fmax = fv;
      *maxIndex = i;
    }
  }

  SET_DOUBLE_VAL(sum, dsum);
  SET_DOUBLE_VAL(max, fmax);
  SET_DOUBLE_VAL(min, fmin);
}

static void getStatics_d(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                         int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  double *data = (double *)pData;
  double  dmin = DBL_MAX;
  double  dmax = -DBL_MAX;
  double  dsum = 0;
  *minIndex = 0;
  *maxIndex = 0;

  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if ((*(uint64_t *)&(data[i])) == TSDB_DATA_DOUBLE_NULL) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
      continue;
    }

    double dv = 0;
    dv = GET_DOUBLE_VAL((const char *)&(data[i]));
    dsum += dv;
    if (dmin > dv) {
      dmin = dv;
      *minIndex = i;
    }

    if (dmax < dv) {
      dmax = dv;
      *maxIndex = i;
    }
  }

  SET_DOUBLE_PTR(sum, &dsum);
  SET_DOUBLE_PTR(max, &dmax);
  SET_DOUBLE_PTR(min, &dmin);
}

static void getStatics_bin(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                           int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  const char *data = pData;
  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (isNull(data, TSDB_DATA_TYPE_BINARY)) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
    }

    data += varDataTLen(data);
  }

  *sum = 0;
  *max = 0;
  *min = 0;
  *minIndex = 0;
  *maxIndex = 0;
}

static void getStatics_nchr(int8_t bitmapMode, const void *pBitmap, const void *pData, int32_t numOfRow, int64_t *min,
                            int64_t *max, int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  const char *data = pData;
  assert(numOfRow <= INT16_MAX);

  for (int32_t i = 0; i < numOfRow; ++i) {
    // if (isNull(data, TSDB_DATA_TYPE_NCHAR)) {
    if (!tdIsBitmapValTypeNorm(pBitmap, i, bitmapMode)) {
      (*numOfNull) += 1;
    }

    data += varDataTLen(data);
  }

  *sum = 0;
  *max = 0;
  *min = 0;
  *minIndex = 0;
  *maxIndex = 0;
}

tDataTypeDescriptor tDataTypes[TSDB_DATA_TYPE_MAX] = {
    {TSDB_DATA_TYPE_NULL, 6, 1, "NOTYPE", 0, 0, NULL, NULL, NULL},
    {TSDB_DATA_TYPE_BOOL, 4, CHAR_BYTES, "BOOL", false, true, tsCompressBool, tsDecompressBool, getStatics_bool},
    {TSDB_DATA_TYPE_TINYINT, 7, CHAR_BYTES, "TINYINT", INT8_MIN, INT8_MAX, tsCompressTinyint, tsDecompressTinyint,
     getStatics_i8},
    {TSDB_DATA_TYPE_SMALLINT, 8, SHORT_BYTES, "SMALLINT", INT16_MIN, INT16_MAX, tsCompressSmallint,
     tsDecompressSmallint, getStatics_i16},
    {TSDB_DATA_TYPE_INT, 3, INT_BYTES, "INT", INT32_MIN, INT32_MAX, tsCompressInt, tsDecompressInt, getStatics_i32},
    {TSDB_DATA_TYPE_BIGINT, 6, LONG_BYTES, "BIGINT", INT64_MIN, INT64_MAX, tsCompressBigint, tsDecompressBigint,
     getStatics_i64},
    {TSDB_DATA_TYPE_FLOAT, 5, FLOAT_BYTES, "FLOAT", 0, 0, tsCompressFloat, tsDecompressFloat, getStatics_f},
    {TSDB_DATA_TYPE_DOUBLE, 6, DOUBLE_BYTES, "DOUBLE", 0, 0, tsCompressDouble, tsDecompressDouble, getStatics_d},
    {TSDB_DATA_TYPE_VARCHAR, 6, 1, "VARCHAR", 0, 0, tsCompressString, tsDecompressString, getStatics_bin},
    {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES, "TIMESTAMP", INT64_MIN, INT64_MAX, tsCompressTimestamp,
     tsDecompressTimestamp, getStatics_i64},
    {TSDB_DATA_TYPE_NCHAR, 5, 1, "NCHAR", 0, 0, tsCompressString, tsDecompressString, getStatics_nchr},
    {TSDB_DATA_TYPE_UTINYINT, 16, CHAR_BYTES, "TINYINT UNSIGNED", 0, UINT8_MAX, tsCompressTinyint, tsDecompressTinyint,
     getStatics_u8},
    {TSDB_DATA_TYPE_USMALLINT, 17, SHORT_BYTES, "SMALLINT UNSIGNED", 0, UINT16_MAX, tsCompressSmallint,
     tsDecompressSmallint, getStatics_u16},
    {TSDB_DATA_TYPE_UINT, 12, INT_BYTES, "INT UNSIGNED", 0, UINT32_MAX, tsCompressInt, tsDecompressInt, getStatics_u32},
    {TSDB_DATA_TYPE_UBIGINT, 15, LONG_BYTES, "BIGINT UNSIGNED", 0, UINT64_MAX, tsCompressBigint, tsDecompressBigint,
     getStatics_u64},
    {TSDB_DATA_TYPE_JSON, 4, TSDB_MAX_JSON_TAG_LEN, "JSON", 0, 0, tsCompressString, tsDecompressString,
     getStatics_nchr},
};

char tTokenTypeSwitcher[13] = {
    TSDB_DATA_TYPE_NULL,     // no type
    TSDB_DATA_TYPE_BINARY,   // TK_ID
    TSDB_DATA_TYPE_BOOL,     // TK_BOOL
    TSDB_DATA_TYPE_BIGINT,   // TK_TINYINT
    TSDB_DATA_TYPE_BIGINT,   // TK_SMALLINT
    TSDB_DATA_TYPE_BIGINT,   // TK_INTEGER
    TSDB_DATA_TYPE_BIGINT,   // TK_BIGINT
    TSDB_DATA_TYPE_DOUBLE,   // TK_FLOAT
    TSDB_DATA_TYPE_DOUBLE,   // TK_DOUBLE
    TSDB_DATA_TYPE_BINARY,   // TK_STRING
    TSDB_DATA_TYPE_BIGINT,   // TK_TIMESTAMP
    TSDB_DATA_TYPE_VARCHAR,  // TK_BINARY
    TSDB_DATA_TYPE_NCHAR,    // TK_NCHAR
};

float  floatMin = -FLT_MAX, floatMax = FLT_MAX;
double doubleMin = -DBL_MAX, doubleMax = DBL_MAX;

FORCE_INLINE void *getDataMin(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      return &floatMin;
    case TSDB_DATA_TYPE_DOUBLE:
      return &doubleMin;
    default:
      return &tDataTypes[type].minValue;
  }
}

FORCE_INLINE void *getDataMax(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      return &floatMax;
    case TSDB_DATA_TYPE_DOUBLE:
      return &doubleMax;
    default:
      return &tDataTypes[type].maxValue;
  }
}

bool isValidDataType(int32_t type) { return type >= TSDB_DATA_TYPE_NULL && type < TSDB_DATA_TYPE_MAX; }

void setVardataNull(void *val, int32_t type) {
  if (type == TSDB_DATA_TYPE_BINARY) {
    varDataSetLen(val, sizeof(int8_t));
    *(uint8_t *)varDataVal(val) = TSDB_DATA_BINARY_NULL;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    varDataSetLen(val, sizeof(int32_t));
    *(uint32_t *)varDataVal(val) = TSDB_DATA_NCHAR_NULL;
  } else {
    assert(0);
  }
}

void setNull(void *val, int32_t type, int32_t bytes) { setNullN(val, type, bytes, 1); }

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))

void setNullN(void *val, int32_t type, int32_t bytes, int32_t numOfElems) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_BOOL_NULL;
      }
      break;
    case TSDB_DATA_TYPE_TINYINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_TINYINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint16_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_SMALLINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_INT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_INT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_BIGINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_UTINYINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint16_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_USMALLINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_UINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_UINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_UBIGINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_FLOAT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_FLOAT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(POINTER_SHIFT(val, i * tDataTypes[type].bytes)) = TSDB_DATA_DOUBLE_NULL;
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY:
      for (int32_t i = 0; i < numOfElems; ++i) {
        setVardataNull(POINTER_SHIFT(val, i * bytes), type);
      }
      break;
    default: {
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(POINTER_SHIFT(val, i * tDataTypes[TSDB_DATA_TYPE_INT].bytes)) = TSDB_DATA_INT_NULL;
      }
      break;
    }
  }
}

static uint8_t      nullBool = TSDB_DATA_BOOL_NULL;
static uint8_t      nullTinyInt = TSDB_DATA_TINYINT_NULL;
static uint16_t     nullSmallInt = TSDB_DATA_SMALLINT_NULL;
static uint32_t     nullInt = TSDB_DATA_INT_NULL;
static uint64_t     nullBigInt = TSDB_DATA_BIGINT_NULL;
static uint32_t     nullFloat = TSDB_DATA_FLOAT_NULL;
static uint64_t     nullDouble = TSDB_DATA_DOUBLE_NULL;
static uint8_t      nullTinyIntu = TSDB_DATA_UTINYINT_NULL;
static uint16_t     nullSmallIntu = TSDB_DATA_USMALLINT_NULL;
static uint32_t     nullIntu = TSDB_DATA_UINT_NULL;
static uint64_t     nullBigIntu = TSDB_DATA_UBIGINT_NULL;
static SBinaryNullT nullBinary = {1, TSDB_DATA_BINARY_NULL};
static SNCharNullT  nullNchar = {4, TSDB_DATA_NCHAR_NULL};

static const void *nullValues[] = {
    &nullBool,   &nullTinyInt, &nullSmallInt, &nullInt,      &nullBigInt,    &nullFloat, &nullDouble,
    &nullBinary, &nullBigInt,  &nullNchar,    &nullTinyIntu, &nullSmallIntu, &nullIntu,  &nullBigIntu,
};

const void *getNullValue(int32_t type) {
  assert(type >= TSDB_DATA_TYPE_BOOL && type <= TSDB_DATA_TYPE_UBIGINT);  // TODO: extend the types
  return nullValues[type - 1];
}

void assignVal(char *val, const char *src, int32_t len, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      *((int8_t *)val) = GET_INT8_VAL(src);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      *((int16_t *)val) = GET_INT16_VAL(src);
      break;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      *((int32_t *)val) = GET_INT32_VAL(src);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      SET_FLOAT_VAL(val, GET_FLOAT_VAL(src));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(val, GET_DOUBLE_VAL(src));
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *((int64_t *)val) = GET_INT64_VAL(src);
      break;
    case TSDB_DATA_TYPE_BINARY:
      varDataCopy(val, src);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      varDataCopy(val, src);
      break;
    default: {
      memcpy(val, src, len);
      break;
    }
  }
}

void operateVal(void *dst, void *s1, void *s2, int32_t optr, int32_t type) {
  if (optr == OP_TYPE_ADD) {
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT:
        *((int8_t *)dst) = GET_INT8_VAL(s1) + GET_INT8_VAL(s2);
        break;
      case TSDB_DATA_TYPE_UTINYINT:
        *((uint8_t *)dst) = GET_UINT8_VAL(s1) + GET_UINT8_VAL(s2);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        *((int16_t *)dst) = GET_INT16_VAL(s1) + GET_INT16_VAL(s2);
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        *((uint16_t *)dst) = GET_UINT16_VAL(s1) + GET_UINT16_VAL(s2);
        break;
      case TSDB_DATA_TYPE_INT:
        *((int32_t *)dst) = GET_INT32_VAL(s1) + GET_INT32_VAL(s2);
        break;
      case TSDB_DATA_TYPE_UINT:
        *((uint32_t *)dst) = GET_UINT32_VAL(s1) + GET_UINT32_VAL(s2);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        *((int64_t *)dst) = GET_INT64_VAL(s1) + GET_INT64_VAL(s2);
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        *((uint64_t *)dst) = GET_UINT64_VAL(s1) + GET_UINT64_VAL(s2);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        *((int64_t *)dst) = GET_INT64_VAL(s1) + GET_INT64_VAL(s2);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        SET_FLOAT_VAL(dst, GET_FLOAT_VAL(s1) + GET_FLOAT_VAL(s2));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        SET_DOUBLE_VAL(dst, GET_DOUBLE_VAL(s1) + GET_DOUBLE_VAL(s2));
        break;
      default: {
        assert(0);
        break;
      }
    }
  } else {
    assert(0);
  }
}

void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size, void *buf) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT: {
      TSWAP(*(int32_t *)(pLeft), *(int32_t *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      TSWAP(*(int64_t *)(pLeft), *(int64_t *)(pRight));
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      TSWAP(*(double *)(pLeft), *(double *)(pRight));
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT: {
      TSWAP(*(int16_t *)(pLeft), *(int16_t *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      TSWAP(*(float *)(pLeft), *(float *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT: {
      TSWAP(*(int8_t *)(pLeft), *(int8_t *)(pRight));
      break;
    }

    default: {
      memcpy(buf, pLeft, size);
      memcpy(pLeft, pRight, size);
      memcpy(pRight, buf, size);
      break;
    }
  }
}
