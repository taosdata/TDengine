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
#include "os.h"

#include "qExecutor.h"
#include "taosmsg.h"
#include "tcompare.h"
#include "ttype.h"

#define FLT_COMPAR_TOL_FACTOR    4
#define FLT_EQUAL(_x, _y)        (fabs((_x) - (_y)) <= (FLT_COMPAR_TOL_FACTOR * FLT_EPSILON))
#define FLT_GREATER(_x, _y)      (!FLT_EQUAL((_x), (_y)) && ((_x) > (_y)))
#define FLT_LESS(_x, _y)         (!FLT_EQUAL((_x), (_y)) && ((_x) < (_y)))
#define FLT_GREATEREQUAL(_x, _y) (FLT_EQUAL((_x), (_y)) || ((_x) > (_y)))
#define FLT_LESSEQUAL(_x, _y)    (FLT_EQUAL((_x), (_y)) || ((_x) < (_y)))

bool lessOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) {
  SColumnFilterInfo* pFilterInfo = &pFilter->filterInfo;

  switch(type) {
    case TSDB_DATA_TYPE_TINYINT: return (*(int8_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UTINYINT: return (*(uint8_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_SMALLINT: return (*(int16_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_USMALLINT: return (*(uint16_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_INT: return (*(int32_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UINT: return (*(uint32_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: return (*(int64_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UBIGINT: return (*(uint64_t *)minval < pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_FLOAT: return FLT_LESS(*(float*)minval, pFilter->filterInfo.upperBndd);
    case TSDB_DATA_TYPE_DOUBLE: return (*(double *)minval < pFilterInfo->upperBndd);
    default:
      return false;
  }
}

bool greaterOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: return (*(int8_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UTINYINT: return (*(uint8_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_SMALLINT: return (*(int16_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_USMALLINT: return (*(uint16_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_INT: return (*(int32_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UINT: return (*(uint32_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: return (*(int64_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UBIGINT: return (*(uint64_t *)maxval > pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_FLOAT: return FLT_GREATER(*(float *)maxval, pFilterInfo->lowerBndd);
    case TSDB_DATA_TYPE_DOUBLE: return (*(double *)maxval > pFilterInfo->lowerBndd);
    default:
      return false;
  }
}

bool lessEqualOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) {
  SColumnFilterInfo* pFilterInfo = &pFilter->filterInfo;

  switch(type) {
    case TSDB_DATA_TYPE_TINYINT: return (*(int8_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UTINYINT: return (*(uint8_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_SMALLINT: return (*(int16_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_USMALLINT: return (*(uint16_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_INT: return (*(int32_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UINT: return (*(uint32_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: return (*(int64_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_UBIGINT: return (*(uint64_t *)minval <= pFilterInfo->upperBndi);
    case TSDB_DATA_TYPE_FLOAT: return FLT_LESSEQUAL(*(float*)minval, pFilterInfo->upperBndd);
    case TSDB_DATA_TYPE_DOUBLE: {
      if ((fabs(*(double*)minval) - pFilterInfo->upperBndd) <= 2 * DBL_EPSILON) {
        return true;
      }

      return (*(double *)minval <= pFilterInfo->upperBndd);
    }
    default:
      return false;
  }
}

bool greaterEqualOperator(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: return (*(int8_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UTINYINT: return (*(uint8_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_SMALLINT: return (*(int16_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_USMALLINT: return (*(uint16_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_INT: return (*(int32_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UINT: return (*(uint32_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: return (*(int64_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_UBIGINT: return (*(uint64_t *)maxval >= pFilterInfo->lowerBndi);
    case TSDB_DATA_TYPE_FLOAT: return FLT_GREATEREQUAL(*(float*)maxval, pFilterInfo->lowerBndd);
    case TSDB_DATA_TYPE_DOUBLE: {
      if (fabs(*(double *)maxval - pFilterInfo->lowerBndd) <= 2 * DBL_EPSILON) {
        return true;
      }

      return (*(double *)maxval - pFilterInfo->lowerBndd > (2 * DBL_EPSILON));
    }
    default:
      return false;
  }
}

////////////////////////////////////////////////////////////////////////
bool equalOperator(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    int64_t minv = -1, maxv = -1;
    GET_TYPED_DATA(minv, int64_t, type, minval);
    GET_TYPED_DATA(maxv, int64_t, type, maxval);

    if (minv == maxv) {
      return minv == pFilterInfo->lowerBndi;
    } else {
      assert(minv < maxv);
      return minv <= pFilterInfo->lowerBndi && pFilterInfo->lowerBndi <= maxv;
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    uint64_t minv = 0, maxv = 0;
    GET_TYPED_DATA(minv, uint64_t, type, minval);
    GET_TYPED_DATA(maxv, uint64_t, type, maxval);

    if (minv == maxv) {
      return minv == pFilterInfo->lowerBndi;
    } else {
      assert(minv < maxv);
      return minv <= pFilterInfo->lowerBndi && pFilterInfo->lowerBndi <= maxv;
    }
  } else if (IS_FLOAT_TYPE(type)) {
    double minv = -1, maxv = -1;
    GET_TYPED_DATA(minv, double, type, minval);
    GET_TYPED_DATA(maxv, double, type, maxval);

    if (minv == maxv) {
      return FLT_EQUAL(minv, pFilterInfo->lowerBndd);
    } else {  // range filter
      assert(minv < maxv);
      return minv <= pFilterInfo->lowerBndd && pFilterInfo->lowerBndd <= maxv;
    }
  } else if (type == TSDB_DATA_TYPE_BINARY) {
    // query condition string is greater than the max length of string, not qualified data
    if (pFilterInfo->len != varDataLen(minval)) {
      return false;
    }

    return strncmp((char *)pFilterInfo->pz, varDataVal(minval), varDataLen(minval)) == 0;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    // query condition string is greater than the max length of string, not qualified data
    if (pFilterInfo->len != varDataLen(minval)) {
      return false;
    }

    return wcsncmp((wchar_t *)pFilterInfo->pz, varDataVal(minval), varDataLen(minval) / TSDB_NCHAR_SIZE) == 0;
  } else {
    return false;
  }
}

////////////////////////////////////////////////////////////////
bool likeOperator(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  if (type == TSDB_DATA_TYPE_BINARY) {
    SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
    return patternMatch((char *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval), &info) == TSDB_PATTERN_MATCH;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
    return WCSPatternMatch((wchar_t*)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)/TSDB_NCHAR_SIZE, &info) == TSDB_PATTERN_MATCH;
  } else {
    return false;
  }
}

////////////////////////////////////////////////////////////////
/**
 *  If minval equals to maxval, it may serve as the one element filter,
 *  or all elements of an array are identical during pref-filter stage.
 *  Otherwise, it must be pre-filter of array list of elements.
 *
 *  During pre-filter stage, if there is one element that locates in [minval, maxval],
 *  the filter function will return true.
 */
// TODO not equal need to refactor
bool notEqualOperator(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    int64_t minv = -1, maxv = -1;
    GET_TYPED_DATA(minv, int64_t, type, minval);
    GET_TYPED_DATA(maxv, int64_t, type, maxval);

    if (minv == maxv) {
      return minv != pFilterInfo->lowerBndi;
    }
    return true;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    uint64_t minv = 0, maxv = 0;
    GET_TYPED_DATA(minv, uint64_t, type, minval);
    GET_TYPED_DATA(maxv, uint64_t, type, maxval);

    if (minv == maxv) {
      return minv != pFilterInfo->lowerBndi;
    }
    return true;
  } else if (IS_FLOAT_TYPE(type)) {
    double minv = -1, maxv = -1;
    GET_TYPED_DATA(minv, double, type, minval);
    GET_TYPED_DATA(maxv, double, type, maxval);

    if (minv == maxv) {
      return !FLT_EQUAL(minv,  pFilterInfo->lowerBndd);
    }
    return true;
  } else if (type == TSDB_DATA_TYPE_BINARY) {
    if (pFilterInfo->len != varDataLen(minval)) {
      return true;
    }
    return strncmp((char *)pFilterInfo->pz, varDataVal(minval), varDataLen(minval)) != 0;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    if (pFilterInfo->len != pFilter->bytes) {
      return true;
    }
    return wcsncmp((wchar_t *)pFilterInfo->pz, varDataVal(minval), varDataLen(minval)/TSDB_NCHAR_SIZE) != 0;
  } else {
    return false;
  }
}

////////////////////////////////////////////////////////////////
// dummy filter, not used
bool isNullOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) {
  return true;
}

bool notNullOperator(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) {
  return true;
}

///////////////////////////////////////////////////////////////////////////////
bool rangeFilter_ii(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      return ((*(int8_t *)minval <= pFilterInfo->upperBndi) && (*(int8_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UTINYINT:
      return ((*(uint8_t *)minval <= pFilterInfo->upperBndi) && (*(uint8_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_SMALLINT:
      return ((*(int16_t *)minval <= pFilterInfo->upperBndi) && (*(int16_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_USMALLINT:
      return ((*(uint16_t *)minval <= pFilterInfo->upperBndi) && (*(uint16_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_INT:
      return ((*(int32_t *)minval <= pFilterInfo->upperBndi) && (*(int32_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UINT:
      return ((*(uint32_t *)minval <= pFilterInfo->upperBndi) && (*(uint32_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      return ((*(int64_t *)minval <= pFilterInfo->upperBndi) && (*(int64_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UBIGINT:
      return ((*(uint64_t *)minval <= pFilterInfo->upperBndi) && (*(uint64_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_FLOAT:
      return FLT_LESSEQUAL(*(float *)minval, pFilterInfo->upperBndd) &&
             FLT_GREATEREQUAL(*(float *)maxval, pFilterInfo->lowerBndd);
    case TSDB_DATA_TYPE_DOUBLE:
      return (*(double *)minval <= pFilterInfo->upperBndd && *(double *)maxval >= pFilterInfo->lowerBndd);
    default:
      return false;
  }
}

bool rangeFilter_ee(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      return ((*(int8_t *)minval < pFilterInfo->upperBndi) && (*(int8_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UTINYINT:
      return ((*(uint8_t *)minval < pFilterInfo->upperBndi) && (*(uint8_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_SMALLINT:
      return ((*(int16_t *)minval < pFilterInfo->upperBndi) && (*(int16_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_USMALLINT:
      return ((*(uint16_t *)minval < pFilterInfo->upperBndi) && (*(uint16_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_INT:
      return ((*(int32_t *)minval < pFilterInfo->upperBndi) && (*(int32_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UINT:
      return ((*(uint32_t *)minval < pFilterInfo->upperBndi) && (*(uint32_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      return ((*(int64_t *)minval < pFilterInfo->upperBndi) && (*(int64_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UBIGINT:
      return ((*(uint64_t *)minval < pFilterInfo->upperBndi) && (*(uint64_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_FLOAT:
      return ((*(float *)minval < pFilterInfo->upperBndd) && (*(float *)maxval > pFilterInfo->lowerBndd));
    case TSDB_DATA_TYPE_DOUBLE:
      return ((*(double *)minval < pFilterInfo->upperBndd) && (*(double *)maxval > pFilterInfo->lowerBndd));
    default:
      return false;
  }
}

bool rangeFilter_ie(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      return ((*(int8_t *)minval < pFilterInfo->upperBndi) && (*(int8_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UTINYINT:
      return ((*(uint8_t *)minval < pFilterInfo->upperBndi) && (*(uint8_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_SMALLINT:
      return ((*(int16_t *)minval < pFilterInfo->upperBndi) && (*(int16_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_USMALLINT:
      return ((*(uint16_t *)minval < pFilterInfo->upperBndi) && (*(uint16_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_INT:
      return ((*(int32_t *)minval < pFilterInfo->upperBndi) && (*(int32_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UINT:
      return ((*(uint32_t *)minval < pFilterInfo->upperBndi) && (*(uint32_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      return ((*(int64_t *)minval < pFilterInfo->upperBndi) && (*(int64_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UBIGINT:
      return ((*(uint64_t *)minval < pFilterInfo->upperBndi) && (*(uint64_t *)maxval >= pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_FLOAT:
      return ((*(float *)minval < pFilterInfo->upperBndd) && (*(float *)maxval >= pFilterInfo->lowerBndd));
    case TSDB_DATA_TYPE_DOUBLE:
      return ((*(double *)minval < pFilterInfo->upperBndd) && (*(double *)maxval >= pFilterInfo->lowerBndd));
    default:
      return false;
  }
}

bool rangeFilter_ei(SColumnFilterElem *pFilter, const char *minval, const char *maxval, int16_t type) {
  SColumnFilterInfo *pFilterInfo = &pFilter->filterInfo;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      return ((*(int8_t *)minval <= pFilterInfo->upperBndi) && (*(int8_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UTINYINT:
      return ((*(uint8_t *)minval <= pFilterInfo->upperBndi) && (*(uint8_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_SMALLINT:
      return ((*(int16_t *)minval <= pFilterInfo->upperBndi) && (*(int16_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_USMALLINT:
      return ((*(uint16_t *)minval <= pFilterInfo->upperBndi) && (*(uint16_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_INT:
      return ((*(int32_t *)minval <= pFilterInfo->upperBndi) && (*(int32_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UINT:
      return ((*(uint32_t *)minval <= pFilterInfo->upperBndi) && (*(uint32_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      return ((*(int64_t *)minval <= pFilterInfo->upperBndi) && (*(int64_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_UBIGINT:
      return ((*(uint64_t *)minval <= pFilterInfo->upperBndi) && (*(uint64_t *)maxval > pFilterInfo->lowerBndi));
    case TSDB_DATA_TYPE_FLOAT:
      return FLT_GREATER(*(float *)maxval, pFilterInfo->lowerBndd) &&
             FLT_LESSEQUAL(*(float *)minval, pFilterInfo->upperBndd);
    case TSDB_DATA_TYPE_DOUBLE:
      return ((*(double *)minval <= pFilterInfo->upperBndd) && (*(double *)maxval > pFilterInfo->lowerBndd));
    default:
      return false;
  }
}

////////////////////////////////////////////////////////////////////////////
bool (*filterOperators[])(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) = {
    NULL,
    lessOperator,
    greaterOperator,
    equalOperator,
    lessEqualOperator,
    greaterEqualOperator,
    notEqualOperator,
    likeOperator,
    isNullOperator,
    notNullOperator,
};

bool (*rangeFilterOperators[])(SColumnFilterElem *pFilter, const char* minval, const char* maxval, int16_t type) = {
    NULL,
    rangeFilter_ee,
    rangeFilter_ie,
    rangeFilter_ei,
    rangeFilter_ii,
};

__filter_func_t getFilterOperator(int32_t lowerOptr, int32_t upperOptr) {
  __filter_func_t funcFp = NULL;

  if ((lowerOptr == TSDB_RELATION_GREATER_EQUAL || lowerOptr == TSDB_RELATION_GREATER) &&
      (upperOptr == TSDB_RELATION_LESS_EQUAL || upperOptr == TSDB_RELATION_LESS)) {
    if (lowerOptr == TSDB_RELATION_GREATER_EQUAL) {
      if (upperOptr == TSDB_RELATION_LESS_EQUAL) {
        funcFp = rangeFilterOperators[4];
      } else {
        funcFp = rangeFilterOperators[2];
      }
    } else {
      if (upperOptr == TSDB_RELATION_LESS_EQUAL) {
        funcFp = rangeFilterOperators[3];
      } else {
        funcFp = rangeFilterOperators[1];
      }
    }
  } else {  // set callback filter function
    if (lowerOptr != TSDB_RELATION_INVALID) {
      funcFp = filterOperators[lowerOptr];

      // invalid filter condition: %d", pQInfo, type
      if (upperOptr != TSDB_RELATION_INVALID) {
        return NULL;
      }
    } else {
      funcFp = filterOperators[upperOptr];
    }
  }

  return funcFp;
}
