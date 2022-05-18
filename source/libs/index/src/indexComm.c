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
#include "ttypes.h"
#include "tvariant.h"

char JSON_COLUMN[] = "JSON";
char JSON_VALUE_DELIM = '&';

char* indexInt2str(int64_t val, char* dst, int radix) {
  char     buffer[65] = {0};
  char*    p;
  int64_t  new_val;
  uint64_t uval = (uint64_t)val;

  if (radix < 0) {
    if (val < 0) {
      *dst++ = '-';
      uval = (uint64_t)0 - uval; /* Avoid integer overflow in (-val) for LLONG_MIN (BUG#31799). */
    }
  }
  p = &buffer[sizeof(buffer) - 1];
  *p = '\0';
  new_val = (int64_t)(uval / 10);
  *--p = '0' + (char)(uval - (uint64_t)new_val * 10);
  val = new_val;

  while (val != 0) {
    new_val = val / 10;
    *--p = '0' + (char)(val - new_val * 10);
    val = new_val;
  }
  while ((*dst++ = *p++) != 0)
    ;
  return dst - 1;
}
static __compar_fn_t indexGetCompar(int8_t type) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    return (__compar_fn_t)strcmp;
  }
  return getComparFunc(type, 0);
}
static TExeCond tCompareLessThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tCompare(func, QUERY_LESS_THAN, a, b, type);
}
static TExeCond tCompareLessEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tCompare(func, QUERY_LESS_EQUAL, a, b, type);
}
static TExeCond tCompareGreaterThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tCompare(func, QUERY_GREATER_THAN, a, b, type);
}
static TExeCond tCompareGreaterEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = indexGetCompar(type);
  return tCompare(func, QUERY_GREATER_EQUAL, a, b, type);
}
TExeCond tCompare(__compar_fn_t func, int8_t cmptype, void* a, void* b, int8_t dtype) {
  if (dtype == TSDB_DATA_TYPE_BINARY || dtype == TSDB_DATA_TYPE_NCHAR || dtype == TSDB_DATA_TYPE_VARBINARY) {
    return tDoCompare(func, cmptype, a, b);
  }
#if 1
  if (dtype == TSDB_DATA_TYPE_TIMESTAMP) {
    int64_t va = taosStr2int64(a);
    int64_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_BOOL || dtype == TSDB_DATA_TYPE_UTINYINT) {
    uint8_t va = taosStr2int64(a);
    uint8_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_TINYINT) {
    int8_t va = taosStr2int64(a);
    int8_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_SMALLINT) {
    int16_t va = taosStr2int64(a);
    int16_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_USMALLINT) {
    uint16_t va = taosStr2int64(a);
    uint16_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_INT) {
    int32_t va = taosStr2int64(a);
    int32_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_UINT) {
    uint32_t va = taosStr2int64(a);
    uint32_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_BIGINT) {
    int64_t va = taosStr2int64(a);
    int64_t vb = taosStr2int64(b);
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_UBIGINT) {
    uint64_t va, vb;
    if (0 != toUInteger(a, strlen(a), 10, &va) || 0 != toUInteger(b, strlen(b), 10, &vb)) {
      return CONTINUE;
    }
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_FLOAT) {
    float va = strtod(a, NULL);
    if (errno == ERANGE && va == -1) {
      return CONTINUE;
    }
    float vb = strtod(b, NULL);
    if (errno == ERANGE && va == -1) {
      return CONTINUE;
    }
    return tDoCompare(func, cmptype, &va, &vb);
  } else if (dtype == TSDB_DATA_TYPE_DOUBLE) {
    double va = strtod(a, NULL);
    if (errno == ERANGE && va == -1) {
      return CONTINUE;
    }
    double vb = strtod(b, NULL);
    if (errno == ERANGE && va == -1) {
      return CONTINUE;
    }
    return tDoCompare(func, cmptype, &va, &vb);
  }
  assert(0);
#endif
}
TExeCond tDoCompare(__compar_fn_t func, int8_t comparType, void* a, void* b) {
  // optime later
  int32_t ret = func(a, b);
  switch (comparType) {
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
      tlen = taosEncodeFixedI64(NULL, *(int64_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedI64(dst, *(int64_t*)src);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      tlen = taosEncodeBinary(NULL, src, sizeof(double));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, sizeof(double));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      tlen = taosEncodeFixedU64(NULL, *(uint64_t*)src);
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeFixedU64(dst, *(uint64_t*)src);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      tlen = taosEncodeBinary(NULL, varDataVal(src), varDataLen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, varDataVal(src), varDataLen(src));

      break;
    }
    case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      break;
    }
    case TSDB_DATA_TYPE_VARBINARY:
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      break;
    default:
      TASSERT(0);
      break;
  }
  *dst = (char*)*dst - tlen;
  // indexMayFillNumbericData(*dst, tlen);
  return tlen;
}
int32_t indexConvertDataToStr(void* src, int8_t type, void** dst) {
  int     tlen = tDataTypes[type].bytes;
  int32_t bufSize = 64;
  switch (type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(int64_t*)src, *dst, -1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(uint8_t*)src, *dst, 1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(int8_t*)src, *dst, 1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(int16_t*)src, *dst, -1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(uint16_t*)src, *dst, -1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_INT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(int32_t*)src, *dst, -1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_UINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(uint32_t*)src, *dst, 1);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      sprintf(*dst, "%" PRIu64, *(uint64_t*)src);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      indexInt2str(*(uint64_t*)src, *dst, 1);
      tlen = strlen(*dst);
    case TSDB_DATA_TYPE_FLOAT:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      sprintf(*dst, "%.9lf", *(float*)src);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *dst = taosMemoryCalloc(1, bufSize + 1);
      sprintf(*dst, "%.9lf", *(double*)src);
      tlen = strlen(*dst);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      tlen = taosEncodeBinary(NULL, varDataVal(src), varDataLen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, varDataVal(src), varDataLen(src));
      *dst = (char*) * dst - tlen;
      break;
    }
    case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      *dst = (char*)*dst - tlen;
      break;
    }
    case TSDB_DATA_TYPE_VARBINARY:
      tlen = taosEncodeBinary(NULL, src, strlen(src));
      *dst = taosMemoryCalloc(1, tlen + 1);
      tlen = taosEncodeBinary(dst, src, strlen(src));
      *dst = (char*)*dst - tlen;
      break;
    default:
      TASSERT(0);
      break;
  }
  return tlen;
}
