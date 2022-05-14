/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "indexComm.h"
#include "index.h"
#include "indexInt.h"
#include "tcoding.h"
#include "tcompare.h"
#include "tdataformat.h"

char JSON_COLUMN[] = "JSON";
char JSON_VALUE_DELIM = '&';

static __compar_fn_t indexGetCompar(int8_t type) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    return (__compar_fn_t)strcmp;
  }
  return getComparFunc(type, 0);
}
static TExeCond tCompareLessThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tDoCommpare(func, QUERY_LESS_THAN, a, b);
}
static TExeCond tCompareLessEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tDoCommpare(func, QUERY_LESS_EQUAL, a, b);
}
static TExeCond tCompareGreaterThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  if (type == TSDB_DATA_TYPE_INT) {
    char* v1 = (char*)a;
    char* v2 = (char*)b;
    for (int i = 0; i < sizeof(int32_t); i++) {
      if (v1[i] == '0') {
        v1[i] = 0;
      }
      if (v2[i] == '0') {
        v2[i] = 0;
      }
    }
    return tDoCommpare(func, QUERY_GREATER_THAN, v1, v2);
  }
  return tDoCommpare(func, QUERY_GREATER_THAN, a, b);
}
static TExeCond tCompareGreaterEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  if (type == TSDB_DATA_TYPE_INT) {
    char* v1 = (char*)a;
    char* v2 = (char*)b;
    for (int i = 0; i < sizeof(int32_t); i++) {
      if (v1[i] == '0') {
        v1[i] = 0;
      }
      if (v2[i] == '0') {
        v2[i] = 0;
      }
    }
    return tDoCommpare(func, QUERY_GREATER_EQUAL, v1, v2);
  }
  return tDoCommpare(func, QUERY_GREATER_EQUAL, a, b);
}

TExeCond tDoCommpare(__compar_fn_t func, int8_t comType, void* a, void* b) {
  // optime later
  int32_t ret = func(a, b);
  switch (comType) {
    case QUERY_LESS_THAN: {
      if (ret < 0) return MATCH;
    } break;
    case QUERY_LESS_EQUAL: {
      if (ret <= 0) return MATCH;
      break;
    }
    case QUERY_GREATER_THAN: {
      if (ret > 0) return MATCH;
      break;
    }
    case QUERY_GREATER_EQUAL: {
      if (ret >= 0) return MATCH;
    }
  }
  return CONTINUE;
}

static TExeCond (*rangeCompare[])(void* a, void* b, int8_t type) = {tCompareLessThan, tCompareLessEqual,
                                                                    tCompareGreaterThan, tCompareGreaterEqual};

_cache_range_compare indexGetCompare(RangeType ty) { return rangeCompare[ty]; }

char* indexPackJsonData(SIndexTerm* itm) {
  /*
   * |<-----colname---->|<-----dataType---->|<--------colVal---------->|
   * |<-----string----->|<-----uint8_t----->|<----depend on dataType-->|
   */
  uint8_t ty = INDEX_TYPE_GET_TYPE(itm->colType);

  int32_t sz = itm->nColName + itm->nColVal + sizeof(uint8_t) + sizeof(JSON_VALUE_DELIM) * 2 + 1;
  char*   buf = (char*)taosMemoryCalloc(1, sz);
  char*   p = buf;

  memcpy(p, itm->colName, itm->nColName);
  p += itm->nColName;

  memcpy(p, &JSON_VALUE_DELIM, sizeof(JSON_VALUE_DELIM));
  p += sizeof(JSON_VALUE_DELIM);

  memcpy(p, &ty, sizeof(ty));
  p += sizeof(ty);

  memcpy(p, &JSON_VALUE_DELIM, sizeof(JSON_VALUE_DELIM));
  p += sizeof(JSON_VALUE_DELIM);

  memcpy(p, itm->colVal, itm->nColVal);

  return buf;
}

char* indexPackJsonDataPrefix(SIndexTerm* itm, int32_t* skip) {
  /*
   * |<-----colname---->|<-----dataType---->|<--------colVal---------->|
   * |<-----string----->|<-----uint8_t----->|<----depend on dataType-->|
   */
  uint8_t ty = INDEX_TYPE_GET_TYPE(itm->colType);

  int32_t sz = itm->nColName + itm->nColVal + sizeof(uint8_t) + sizeof(JSON_VALUE_DELIM) * 2 + 1;
  char*   buf = (char*)taosMemoryCalloc(1, sz);
  char*   p = buf;

  memcpy(p, itm->colName, itm->nColName);
  p += itm->nColName;

  memcpy(p, &JSON_VALUE_DELIM, sizeof(JSON_VALUE_DELIM));
  p += sizeof(JSON_VALUE_DELIM);

  memcpy(p, &ty, sizeof(ty));
  p += sizeof(ty);

  memcpy(p, &JSON_VALUE_DELIM, sizeof(JSON_VALUE_DELIM));
  p += sizeof(JSON_VALUE_DELIM);

  *skip = p - buf;

  return buf;
}

int32_t indexConvertData(void* src, int8_t type, void** dst) {
  int tlen = -1;
  switch (type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      tlen = taosEncodeFixedI64(NULL, *(int64_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI64(dst, *(int64_t*)src);
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
      tlen = taosEncodeFixedU8(NULL, *(uint8_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedU8(dst, *(uint8_t*)src);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      tlen = taosEncodeFixedI8(NULL, *(uint8_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI8(dst, *(uint8_t*)src);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      tlen = taosEncodeFixedI16(NULL, *(int16_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI16(dst, *(int16_t*)src);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      tlen = taosEncodeFixedU16(NULL, *(uint16_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedU16(dst, *(uint16_t*)src);
      break;
    case TSDB_DATA_TYPE_INT:
      tlen = taosEncodeFixedI32(NULL, *(int32_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI32(dst, *(int32_t*)src);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      tlen = taosEncodeBinary(NULL, src, sizeof(float));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, sizeof(float));
      break;
    case TSDB_DATA_TYPE_UINT:
      tlen = taosEncodeFixedU32(NULL, *(uint32_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedU32(dst, *(uint32_t*)src);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      tlen = taosEncodeFixedI64(NULL, *(uint32_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI64(dst, *(uint32_t*)src);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      tlen = taosEncodeBinary(NULL, src, sizeof(double));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, sizeof(double));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      tlen = taosEncodeFixedU64(NULL, *(uint32_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedU64(dst, *(uint32_t*)src);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      tlen = taosEncodeBinary(NULL, varDataVal(src), varDataLen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, varDataVal(src), varDataLen(src));

      break;
    }
    case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
#if 1
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      break;
#endif
    }
    case TSDB_DATA_TYPE_VARBINARY:
#if 1
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      break;
#endif
    default:
      TASSERT(0);
      break;
  }
  *dst = *dst - tlen;
  if (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR && type != TSDB_DATA_TYPE_VARBINARY &&
      type != TSDB_DATA_TYPE_VARCHAR) {
    uint8_t* p = *dst;
    for (int i = 0; i < tlen; i++) {
      if (p[i] == 0) {
        p[i] = (uint8_t)'0';
      }
    }
  }
  return tlen;
}
