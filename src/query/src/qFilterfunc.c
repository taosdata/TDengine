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
#include "tsqlfunction.h"

bool less_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval < pFilter->filterInfo.upperBndi);
}

bool less_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval < pFilter->filterInfo.upperBndi);
}

bool less_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval < pFilter->filterInfo.upperBndi);
}

bool less_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval < pFilter->filterInfo.upperBndi);
}

bool less_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(float *)minval - pFilter->filterInfo.upperBndd < (2 * FLT_EPSILON));
}

bool less_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)minval - pFilter->filterInfo.upperBndd < (2 * DBL_EPSILON));
}

//////////////////////////////////////////////////////////////////
bool larger_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)maxval > pFilter->filterInfo.lowerBndi);
}

bool larger_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)maxval > pFilter->filterInfo.lowerBndi);
}

bool larger_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)maxval > pFilter->filterInfo.lowerBndi);
}

bool larger_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)maxval > pFilter->filterInfo.lowerBndi);
}

bool larger_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return ((*(float *)maxval - pFilter->filterInfo.lowerBndd) > (2 * FLT_EPSILON));
}

bool larger_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)maxval - pFilter->filterInfo.lowerBndd) > (2 * DBL_EPSILON);
}
/////////////////////////////////////////////////////////////////////

bool lessEqual_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->filterInfo.upperBndi);
}

bool lessEqual_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->filterInfo.upperBndi);
}

bool lessEqual_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->filterInfo.upperBndi);
}

bool lessEqual_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->filterInfo.upperBndi);
}

bool lessEqual_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (fabs(*(float*)minval - pFilter->filterInfo.upperBndd) <= 2 * FLT_EPSILON) {
    return true;
  }

  return (*(float *)minval <= pFilter->filterInfo.upperBndd);
}

bool lessEqual_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if ((fabs(*(double*)minval) - pFilter->filterInfo.upperBndd) <= 2 * DBL_EPSILON) {
    return true;
  }

  return (*(double *)minval <= pFilter->filterInfo.upperBndd);
}

//////////////////////////////////////////////////////////////////////////
bool largeEqual_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool largeEqual_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool largeEqual_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool largeEqual_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool largeEqual_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (fabs(*(float*)minval - pFilter->filterInfo.upperBndd) <= (2 * FLT_EPSILON)) {
    return true;
  }

  return (*(float *)maxval - pFilter->filterInfo.lowerBndd > (2 * FLT_EPSILON));
}

bool largeEqual_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (fabs(*(double *)maxval - pFilter->filterInfo.lowerBndd) <= 2 * DBL_EPSILON) {
    return true;
  }

  return (*(double *)maxval - pFilter->filterInfo.lowerBndd > (2 * DBL_EPSILON));
}

////////////////////////////////////////////////////////////////////////

bool equal_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int8_t *)minval == *(int8_t *)maxval) {
    return (*(int8_t *)minval == pFilter->filterInfo.lowerBndi);
  } else { /* range filter */
    assert(*(int8_t *)minval < *(int8_t *)maxval);

    return *(int8_t *)minval <= pFilter->filterInfo.lowerBndi && *(int8_t *)maxval >= pFilter->filterInfo.lowerBndi;
  }
}

bool equal_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int16_t *)minval == *(int16_t *)maxval) {
    return (*(int16_t *)minval == pFilter->filterInfo.lowerBndi);
  } else { /* range filter */
    assert(*(int16_t *)minval < *(int16_t *)maxval);

    return *(int16_t *)minval <= pFilter->filterInfo.lowerBndi && *(int16_t *)maxval >= pFilter->filterInfo.lowerBndi;
  }
}

bool equal_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int32_t *)minval == *(int32_t *)maxval) {
    return (*(int32_t *)minval == pFilter->filterInfo.lowerBndi);
  } else { /* range filter */
    assert(*(int32_t *)minval < *(int32_t *)maxval);

    return *(int32_t *)minval <= pFilter->filterInfo.lowerBndi && *(int32_t *)maxval >= pFilter->filterInfo.lowerBndi;
  }
}

bool equal_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int64_t *)minval == *(int64_t *)maxval) {
    return (*(int64_t *)minval == pFilter->filterInfo.lowerBndi);
  } else { /* range filter */
    assert(*(int64_t *)minval < *(int64_t *)maxval);

    return *(int64_t *)minval <= pFilter->filterInfo.lowerBndi && *(int64_t *)maxval >= pFilter->filterInfo.lowerBndi;
  }
}

// user specified input filter value and the original saved float value may needs to
// increase the tolerance to obtain the correct result.
bool equal_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(float *)minval == *(float *)maxval) {
    return (fabs(*(float *)minval - pFilter->filterInfo.lowerBndd) <= FLT_EPSILON * 2);
  } else { // range filter
    assert(*(float *)minval < *(float *)maxval);
    return *(float *)minval <= pFilter->filterInfo.lowerBndd && *(float *)maxval >= pFilter->filterInfo.lowerBndd;
  }
}

bool equal_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(double *)minval == *(double *)maxval) {
    return (fabs(*(double *)minval - pFilter->filterInfo.lowerBndd) <= 2 * DBL_EPSILON);
  } else { // range filter
    assert(*(double *)minval < *(double *)maxval);
    return *(double *)minval <= pFilter->filterInfo.lowerBndi && *(double *)maxval >= pFilter->filterInfo.lowerBndi;
  }
}

bool equal_str(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  // query condition string is greater than the max length of string, not qualified data
  if (pFilter->filterInfo.len != varDataLen(minval)) {
    return false;
  }

  return strncmp((char *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)) == 0;
}

bool equal_nchar(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  // query condition string is greater than the max length of string, not qualified data
  if (pFilter->filterInfo.len != varDataLen(minval)) {
    return false;
  }

  return wcsncmp((wchar_t *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)/TSDB_NCHAR_SIZE) == 0;
}

////////////////////////////////////////////////////////////////
bool like_str(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  return patternMatch((char *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval), &info) == TSDB_PATTERN_MATCH;
}

bool like_nchar(SColumnFilterElem* pFilter, char* minval, char *maxval) {
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  
  return WCSPatternMatch((wchar_t*)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)/TSDB_NCHAR_SIZE, &info) == TSDB_PATTERN_MATCH;
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
bool nequal_i8(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int8_t *)minval == *(int8_t *)maxval) {
    return (*(int8_t *)minval != pFilter->filterInfo.lowerBndi);
  }

  return true;
}

bool nequal_i16(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int16_t *)minval == *(int16_t *)maxval) {
    return (*(int16_t *)minval != pFilter->filterInfo.lowerBndi);
  }

  return true;
}

bool nequal_i32(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int32_t *)minval == *(int32_t *)maxval) {
    return (*(int32_t *)minval != pFilter->filterInfo.lowerBndi);
  }

  return true;
}

bool nequal_i64(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(int64_t *)minval == *(int64_t *)maxval) {
    return (*(int64_t *)minval != pFilter->filterInfo.lowerBndi);
  }

  return true;
}

bool nequal_ds(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(float *)minval == *(float *)maxval) {
    return ((fabs(*(float *)minval - pFilter->filterInfo.lowerBndd)) > (2 * FLT_EPSILON));
  }

  return true;
}

bool nequal_dd(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (*(double *)minval == *(double *)maxval) {
    return (*(double *)minval != pFilter->filterInfo.lowerBndd);
  }

  return true;
}

bool nequal_str(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  if (pFilter->filterInfo.len != varDataLen(minval)) {
    return true;
  }

  return strncmp((char *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)) != 0;
}

bool nequal_nchar(SColumnFilterElem *pFilter, char* minval, char *maxval) {
  if (pFilter->filterInfo.len > pFilter->bytes) {
    return true;
  }

  return wcsncmp((wchar_t *)pFilter->filterInfo.pz, varDataVal(minval), varDataLen(minval)/TSDB_NCHAR_SIZE) != 0;
}
////////////////////////////////////////////////////////////////
bool isNull_filter(SColumnFilterElem *pFilter, char* minval, char* maxval) {
  return true;
}

bool notNull_filter(SColumnFilterElem *pFilter, char* minval, char* maxval) {
  return true;
}

////////////////////////////////////////////////////////////////

bool rangeFilter_i32_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->filterInfo.upperBndi && *(int32_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i32_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval<pFilter->filterInfo.upperBndi &&*(int32_t *)maxval> pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i32_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval < pFilter->filterInfo.upperBndi && *(int32_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i32_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->filterInfo.upperBndi && *(int32_t *)maxval > pFilter->filterInfo.lowerBndi);
}

///////////////////////////////////////////////////////////////////////////////
bool rangeFilter_i8_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->filterInfo.upperBndi && *(int8_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i8_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval<pFilter->filterInfo.upperBndi &&*(int8_t *)maxval> pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i8_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval < pFilter->filterInfo.upperBndi && *(int8_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i8_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->filterInfo.upperBndi && *(int8_t *)maxval > pFilter->filterInfo.lowerBndi);
}

/////////////////////////////////////////////////////////////////////////////////////
bool rangeFilter_i16_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->filterInfo.upperBndi && *(int16_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i16_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval<pFilter->filterInfo.upperBndi &&*(int16_t *)maxval> pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i16_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval < pFilter->filterInfo.upperBndi && *(int16_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i16_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->filterInfo.upperBndi && *(int16_t *)maxval > pFilter->filterInfo.lowerBndi);
}

////////////////////////////////////////////////////////////////////////
bool rangeFilter_i64_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->filterInfo.upperBndi && *(int64_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i64_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval<pFilter->filterInfo.upperBndi &&*(int64_t *)maxval> pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i64_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval < pFilter->filterInfo.upperBndi && *(int64_t *)maxval >= pFilter->filterInfo.lowerBndi);
}

bool rangeFilter_i64_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->filterInfo.upperBndi && *(int64_t *)maxval > pFilter->filterInfo.lowerBndi);
}

////////////////////////////////////////////////////////////////////////
bool rangeFilter_ds_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(float *)minval <= pFilter->filterInfo.upperBndd && *(float *)maxval >= pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_ds_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(float *)minval<pFilter->filterInfo.upperBndd &&*(float *)maxval> pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_ds_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(float *)minval < pFilter->filterInfo.upperBndd && *(float *)maxval >= pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_ds_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(float *)minval <= pFilter->filterInfo.upperBndd && *(float *)maxval > pFilter->filterInfo.lowerBndd);
}

//////////////////////////////////////////////////////////////////////////
bool rangeFilter_dd_ii(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)minval <= pFilter->filterInfo.upperBndd && *(double *)maxval >= pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_dd_ee(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)minval<pFilter->filterInfo.upperBndd &&*(double *)maxval> pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_dd_ie(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)minval < pFilter->filterInfo.upperBndd && *(double *)maxval >= pFilter->filterInfo.lowerBndd);
}

bool rangeFilter_dd_ei(SColumnFilterElem *pFilter, char *minval, char *maxval) {
  return (*(double *)minval <= pFilter->filterInfo.upperBndd && *(double *)maxval > pFilter->filterInfo.lowerBndd);
}

////////////////////////////////////////////////////////////////////////////
bool (*filterFunc_i8[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_i8,
  larger_i8,
  equal_i8,
  lessEqual_i8,
  largeEqual_i8,
  nequal_i8,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_i16[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_i16,
  larger_i16,
  equal_i16,
  lessEqual_i16,
  largeEqual_i16,
  nequal_i16,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_i32[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_i32,
  larger_i32,
  equal_i32,
  lessEqual_i32,
  largeEqual_i32,
  nequal_i32,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_i64[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_i64,
  larger_i64,
  equal_i64,
  lessEqual_i64,
  largeEqual_i64,
  nequal_i64,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_ds[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_ds,
  larger_ds,
  equal_ds,
  lessEqual_ds,
  largeEqual_ds,
  nequal_ds,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_dd[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  less_dd,
  larger_dd,
  equal_dd,
  lessEqual_dd,
  largeEqual_dd,
  nequal_dd,
  NULL,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_str[])(SColumnFilterElem* pFilter, char* minval, char *maxval) = {
  NULL,
  NULL,
  NULL,
  equal_str,
  NULL,
  NULL,
  nequal_str,
  like_str,
  isNull_filter,
  notNull_filter,
};

bool (*filterFunc_nchar[])(SColumnFilterElem* pFitler, char* minval, char* maxval) = {
  NULL,
  NULL,
  NULL,
  equal_nchar,
  NULL,
  NULL,
  nequal_nchar,
  like_nchar,
  isNull_filter,
  notNull_filter,
};

bool (*rangeFilterFunc_i8[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_i8_ee,
  rangeFilter_i8_ie,
  rangeFilter_i8_ei,
  rangeFilter_i8_ii,
};

bool (*rangeFilterFunc_i16[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_i16_ee,
  rangeFilter_i16_ie,
  rangeFilter_i16_ei,
  rangeFilter_i16_ii,
};

bool (*rangeFilterFunc_i32[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_i32_ee,
  rangeFilter_i32_ie,
  rangeFilter_i32_ei,
  rangeFilter_i32_ii,
};

bool (*rangeFilterFunc_i64[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_i64_ee,
  rangeFilter_i64_ie,
  rangeFilter_i64_ei,
  rangeFilter_i64_ii,
};

bool (*rangeFilterFunc_ds[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_ds_ee,
  rangeFilter_ds_ie,
  rangeFilter_ds_ei,
  rangeFilter_ds_ii,
};

bool (*rangeFilterFunc_dd[])(SColumnFilterElem *pFilter, char *minval, char *maxval) = {
  NULL,
  rangeFilter_dd_ee,
  rangeFilter_dd_ie,
  rangeFilter_dd_ei,
  rangeFilter_dd_ii,
};

__filter_func_t* getRangeFilterFuncArray(int32_t type) {
  switch(type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:    return rangeFilterFunc_i8;
    case TSDB_DATA_TYPE_SMALLINT:   return rangeFilterFunc_i16;
    case TSDB_DATA_TYPE_INT:        return rangeFilterFunc_i32;
    case TSDB_DATA_TYPE_TIMESTAMP:  //timestamp uses bigint filter
    case TSDB_DATA_TYPE_BIGINT:     return rangeFilterFunc_i64;
    case TSDB_DATA_TYPE_FLOAT:      return rangeFilterFunc_ds;
    case TSDB_DATA_TYPE_DOUBLE:     return rangeFilterFunc_dd;
    default:return NULL;
  }
}

__filter_func_t* getValueFilterFuncArray(int32_t type) {
  switch(type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:    return filterFunc_i8;
    case TSDB_DATA_TYPE_SMALLINT:   return filterFunc_i16;
    case TSDB_DATA_TYPE_INT:        return filterFunc_i32;
    case TSDB_DATA_TYPE_TIMESTAMP:  //timestamp uses bigint filter
    case TSDB_DATA_TYPE_BIGINT:     return filterFunc_i64;
    case TSDB_DATA_TYPE_FLOAT:      return filterFunc_ds;
    case TSDB_DATA_TYPE_DOUBLE:     return filterFunc_dd;
    case TSDB_DATA_TYPE_BINARY:     return filterFunc_str;
    case TSDB_DATA_TYPE_NCHAR:      return filterFunc_nchar;
    default: return NULL;
  }
}
