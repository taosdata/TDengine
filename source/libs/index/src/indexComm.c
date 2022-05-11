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
#include "tcompare.h"

char JSON_COLUMN[] = "JSON";
char JSON_VALUE_DELIM = '&';

static TExeCond tCompareLessThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = getComparFunc(type, 0);
  return tDoCommpare(func, QUERY_LESS_THAN, a, b);
}
static TExeCond tCompareLessEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = getComparFunc(type, 0);
  return tDoCommpare(func, QUERY_LESS_EQUAL, a, b);
}
static TExeCond tCompareGreaterThan(void* a, void* b, int8_t type) {
  __compar_fn_t func = getComparFunc(type, 0);
  return tDoCommpare(func, QUERY_GREATER_THAN, a, b);
}
static TExeCond tCompareGreaterEqual(void* a, void* b, int8_t type) {
  __compar_fn_t func = getComparFunc(type, 0);
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
