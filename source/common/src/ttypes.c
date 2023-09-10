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

const int32_t TYPE_BYTES[21] = {
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
    TSDB_MAX_TAGS_LEN,       // TSDB_DATA_TYPE_VARBINARY: placeholder, not implemented
    TSDB_MAX_TAGS_LEN,       // TSDB_DATA_TYPE_DECIMAL: placeholder, not implemented
    TSDB_MAX_TAGS_LEN,       // TSDB_DATA_TYPE_BLOB: placeholder, not implemented
    TSDB_MAX_TAGS_LEN,       // TSDB_DATA_TYPE_MEDIUMBLOB: placeholder, not implemented
    sizeof(VarDataOffsetT),  // TSDB_DATA_TYPE_GEOMETRY
};

tDataTypeDescriptor tDataTypes[TSDB_DATA_TYPE_MAX] = {
    {TSDB_DATA_TYPE_NULL, 6, 1, "NOTYPE", 0, 0, NULL, NULL},
    {TSDB_DATA_TYPE_BOOL, 4, CHAR_BYTES, "BOOL", false, true, tsCompressBool, tsDecompressBool},
    {TSDB_DATA_TYPE_TINYINT, 7, CHAR_BYTES, "TINYINT", INT8_MIN, INT8_MAX, tsCompressTinyint, tsDecompressTinyint},
    {TSDB_DATA_TYPE_SMALLINT, 8, SHORT_BYTES, "SMALLINT", INT16_MIN, INT16_MAX, tsCompressSmallint,
     tsDecompressSmallint},
    {TSDB_DATA_TYPE_INT, 3, INT_BYTES, "INT", INT32_MIN, INT32_MAX, tsCompressInt, tsDecompressInt},
    {TSDB_DATA_TYPE_BIGINT, 6, LONG_BYTES, "BIGINT", INT64_MIN, INT64_MAX, tsCompressBigint, tsDecompressBigint},
    {TSDB_DATA_TYPE_FLOAT, 5, FLOAT_BYTES, "FLOAT", 0, 0, tsCompressFloat, tsDecompressFloat},
    {TSDB_DATA_TYPE_DOUBLE, 6, DOUBLE_BYTES, "DOUBLE", 0, 0, tsCompressDouble, tsDecompressDouble},
    {TSDB_DATA_TYPE_VARCHAR, 6, 1, "VARCHAR", 0, 0, tsCompressString, tsDecompressString},
    {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES, "TIMESTAMP", INT64_MIN, INT64_MAX, tsCompressTimestamp,
     tsDecompressTimestamp},
    {TSDB_DATA_TYPE_NCHAR, 5, 1, "NCHAR", 0, 0, tsCompressString, tsDecompressString},
    {TSDB_DATA_TYPE_UTINYINT, 16, CHAR_BYTES, "TINYINT UNSIGNED", 0, UINT8_MAX, tsCompressTinyint, tsDecompressTinyint},
    {TSDB_DATA_TYPE_USMALLINT, 17, SHORT_BYTES, "SMALLINT UNSIGNED", 0, UINT16_MAX, tsCompressSmallint,
     tsDecompressSmallint},
    {TSDB_DATA_TYPE_UINT, 12, INT_BYTES, "INT UNSIGNED", 0, UINT32_MAX, tsCompressInt, tsDecompressInt},
    {TSDB_DATA_TYPE_UBIGINT, 15, LONG_BYTES, "BIGINT UNSIGNED", 0, UINT64_MAX, tsCompressBigint, tsDecompressBigint},
    {TSDB_DATA_TYPE_JSON, 4, TSDB_MAX_JSON_TAG_LEN, "JSON", 0, 0, tsCompressString, tsDecompressString},
    {TSDB_DATA_TYPE_VARBINARY, 9, 1, "VARBINARY", 0, 0, tsCompressString, tsDecompressString},     // placeholder, not implemented
    {TSDB_DATA_TYPE_DECIMAL, 7, 1, "DECIMAL", 0, 0, NULL, NULL},         // placeholder, not implemented
    {TSDB_DATA_TYPE_BLOB, 4, 1, "BLOB", 0, 0, NULL, NULL},               // placeholder, not implemented
    {TSDB_DATA_TYPE_MEDIUMBLOB, 10, 1, "MEDIUMBLOB", 0, 0, NULL, NULL},  // placeholder, not implemented
    {TSDB_DATA_TYPE_GEOMETRY, 8, 1, "GEOMETRY", 0, 0, tsCompressString, tsDecompressString},
};

static float  floatMin = -FLT_MAX, floatMax = FLT_MAX;
static double doubleMin = -DBL_MAX, doubleMax = DBL_MAX;

FORCE_INLINE void *getDataMin(int32_t type, void *value) {
  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      *(float *)value = floatMin;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *(double *)value = doubleMin;
      break;
    default:
      *(int64_t *)value = tDataTypes[type].minValue;
      break;
  }

  return value;
}

FORCE_INLINE void *getDataMax(int32_t type, void *value) {
  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      *(float *)value = floatMax;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *(double *)value = doubleMax;
      break;
    default:
      *(int64_t *)value = tDataTypes[type].maxValue;
      break;
  }

  return value;
}

bool isValidDataType(int32_t type) { return type >= TSDB_DATA_TYPE_NULL && type < TSDB_DATA_TYPE_MAX; }

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))

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
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      varDataCopy(val, src);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      varDataCopy(val, src);
      break;
    default: {
      if (len > 0) {
        memcpy(val, src, len);
      }

      break;
    }
  }
}

int32_t operateVal(void *dst, void *s1, void *s2, int32_t optr, int32_t type) {
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
        return -1;
      }
    }
  } else {
    return -1;
  }

  return 0;
}
