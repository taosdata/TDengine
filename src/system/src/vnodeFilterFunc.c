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

#include <float.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "taosmsg.h"
#include "tsqlfunction.h"
#include "vnode.h"
#include "vnodeDataFilterFunc.h"

bool less_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval < pFilter->data.upperBndi);
}

bool less_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval < pFilter->data.upperBndi);
}

bool less_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval < pFilter->data.upperBndi);
}

bool less_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval < pFilter->data.upperBndi);
}

bool less_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval < pFilter->data.upperBndd);
}

bool less_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval < pFilter->data.upperBndd);
}

//////////////////////////////////////////////////////////////////
bool large_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)maxval > pFilter->data.lowerBndi);
}

bool large_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)maxval > pFilter->data.lowerBndi);
}

bool large_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)maxval > pFilter->data.lowerBndi);
}

bool large_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)maxval > pFilter->data.lowerBndi);
}

bool large_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)maxval > pFilter->data.lowerBndd);
}

bool large_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)maxval > pFilter->data.lowerBndd);
}
/////////////////////////////////////////////////////////////////////

bool lessEqual_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->data.upperBndi);
}

bool lessEqual_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->data.upperBndi);
}

bool lessEqual_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->data.upperBndi);
}

bool lessEqual_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->data.upperBndi);
}

bool lessEqual_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval <= pFilter->data.upperBndd);
}

bool lessEqual_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval <= pFilter->data.upperBndd);
}

//////////////////////////////////////////////////////////////////////////
bool largeEqual_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)maxval >= pFilter->data.lowerBndi);
}

bool largeEqual_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)maxval >= pFilter->data.lowerBndi);
}

bool largeEqual_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)maxval >= pFilter->data.lowerBndi);
}

bool largeEqual_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)maxval >= pFilter->data.lowerBndi);
}

bool largeEqual_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)maxval >= pFilter->data.lowerBndd);
}

bool largeEqual_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)maxval >= pFilter->data.lowerBndd);
}

////////////////////////////////////////////////////////////////////////

bool equal_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int8_t *)minval == *(int8_t *)maxval) {
    return (*(int8_t *)minval == pFilter->data.lowerBndi);
  } else { /* range filter */
    assert(*(int8_t *)minval < *(int8_t *)maxval);

    return *(int8_t *)minval <= pFilter->data.lowerBndi && *(int8_t *)maxval >= pFilter->data.lowerBndi;
  }
}

bool equal_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int16_t *)minval == *(int16_t *)maxval) {
    return (*(int16_t *)minval == pFilter->data.lowerBndi);
  } else { /* range filter */
    assert(*(int16_t *)minval < *(int16_t *)maxval);

    return *(int16_t *)minval <= pFilter->data.lowerBndi && *(int16_t *)maxval >= pFilter->data.lowerBndi;
  }
}

bool equal_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int32_t *)minval == *(int32_t *)maxval) {
    return (*(int32_t *)minval == pFilter->data.lowerBndi);
  } else { /* range filter */
    assert(*(int32_t *)minval < *(int32_t *)maxval);

    return *(int32_t *)minval <= pFilter->data.lowerBndi && *(int32_t *)maxval >= pFilter->data.lowerBndi;
  }
}

bool equal_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int64_t *)minval == *(int64_t *)maxval) {
    return (*(int64_t *)minval == pFilter->data.lowerBndi);
  } else { /* range filter */
    assert(*(int64_t *)minval < *(int64_t *)maxval);

    return *(int64_t *)minval <= pFilter->data.lowerBndi && *(int64_t *)maxval >= pFilter->data.lowerBndi;
  }
}

bool equal_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(float *)minval == *(float *)maxval) {
    return (fabs(*(float *)minval - pFilter->data.lowerBndd) <= FLT_EPSILON);
  } else { /* range filter */
    assert(*(float *)minval < *(float *)maxval);
    return *(float *)minval <= pFilter->data.lowerBndd && *(float *)maxval >= pFilter->data.lowerBndd;
  }
}

bool equal_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(double *)minval == *(double *)maxval) {
    return (*(double *)minval == pFilter->data.lowerBndd);
  } else { /* range filter */
    assert(*(double *)minval < *(double *)maxval);

    return *(double *)minval <= pFilter->data.lowerBndi && *(double *)maxval >= pFilter->data.lowerBndi;
  }
}

bool equal_str(SColumnFilter *pFilter, char *minval, char *maxval) {
  // query condition string is greater than the max length of string, not qualified data
  if (pFilter->data.len > pFilter->data.bytes) {
    return false;
  }

  return strncmp((char *)pFilter->data.pz, minval, pFilter->data.bytes) == 0;
}

////////////////////////////////////////////////////////////////
bool like_str(SColumnFilter *pFilter, char *minval, char *maxval) {
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  return patternMatch((char *)pFilter->data.pz, minval, pFilter->data.bytes, &info) == TSDB_PATTERN_MATCH;
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
bool nequal_i8(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int8_t *)minval == *(int8_t *)maxval) {
    return (*(int8_t *)minval != pFilter->data.lowerBndi);
  }

  return true;
}

bool nequal_i16(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int16_t *)minval == *(int16_t *)maxval) {
    return (*(int16_t *)minval != pFilter->data.lowerBndi);
  }

  return true;
}

bool nequal_i32(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int32_t *)minval == *(int32_t *)maxval) {
    return (*(int32_t *)minval != pFilter->data.lowerBndi);
  }

  return true;
}

bool nequal_i64(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(int64_t *)minval == *(int64_t *)maxval) {
    return (*(int64_t *)minval != pFilter->data.lowerBndi);
  }

  return true;
}

bool nequal_ds(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(float *)minval == *(float *)maxval) {
    return (*(float *)minval != pFilter->data.lowerBndd);
  }

  return true;
}

bool nequal_dd(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (*(double *)minval == *(double *)maxval) {
    return (*(double *)minval != pFilter->data.lowerBndd);
  }

  return true;
}

bool nequal_str(SColumnFilter *pFilter, char *minval, char *maxval) {
  if (pFilter->data.len > pFilter->data.bytes) {
    return true;
  }

  return strncmp((char *)pFilter->data.pz, minval, pFilter->data.bytes) != 0;
}

////////////////////////////////////////////////////////////////

bool rangeFilter_i32_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->data.upperBndi && *(int32_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i32_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval<pFilter->data.upperBndi &&*(int32_t *)maxval> pFilter->data.lowerBndi);
}

bool rangeFilter_i32_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval < pFilter->data.upperBndi && *(int32_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i32_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int32_t *)minval <= pFilter->data.upperBndi && *(int32_t *)maxval > pFilter->data.lowerBndi);
}

///////////////////////////////////////////////////////////////////////////////
bool rangeFilter_i8_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->data.upperBndi && *(int8_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i8_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval<pFilter->data.upperBndi &&*(int8_t *)maxval> pFilter->data.lowerBndi);
}

bool rangeFilter_i8_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval < pFilter->data.upperBndi && *(int8_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i8_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int8_t *)minval <= pFilter->data.upperBndi && *(int8_t *)maxval > pFilter->data.lowerBndi);
}

/////////////////////////////////////////////////////////////////////////////////////
bool rangeFilter_i16_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->data.upperBndi && *(int16_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i16_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval<pFilter->data.upperBndi &&*(int16_t *)maxval> pFilter->data.lowerBndi);
}

bool rangeFilter_i16_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval < pFilter->data.upperBndi && *(int16_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i16_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int16_t *)minval <= pFilter->data.upperBndi && *(int16_t *)maxval > pFilter->data.lowerBndi);
}

////////////////////////////////////////////////////////////////////////
bool rangeFilter_i64_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->data.upperBndi && *(int64_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i64_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval<pFilter->data.upperBndi &&*(int64_t *)maxval> pFilter->data.lowerBndi);
}

bool rangeFilter_i64_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval < pFilter->data.upperBndi && *(int64_t *)maxval >= pFilter->data.lowerBndi);
}

bool rangeFilter_i64_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(int64_t *)minval <= pFilter->data.upperBndi && *(int64_t *)maxval > pFilter->data.lowerBndi);
}

////////////////////////////////////////////////////////////////////////
bool rangeFilter_ds_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval <= pFilter->data.upperBndd && *(float *)maxval >= pFilter->data.lowerBndd);
}

bool rangeFilter_ds_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval<pFilter->data.upperBndd &&*(float *)maxval> pFilter->data.lowerBndd);
}

bool rangeFilter_ds_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval < pFilter->data.upperBndd && *(float *)maxval >= pFilter->data.lowerBndd);
}

bool rangeFilter_ds_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(float *)minval <= pFilter->data.upperBndd && *(float *)maxval > pFilter->data.lowerBndd);
}

//////////////////////////////////////////////////////////////////////////
bool rangeFilter_dd_ii(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval <= pFilter->data.upperBndd && *(double *)maxval >= pFilter->data.lowerBndd);
}

bool rangeFilter_dd_ee(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval<pFilter->data.upperBndd &&*(double *)maxval> pFilter->data.lowerBndd);
}

bool rangeFilter_dd_ie(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval < pFilter->data.upperBndd && *(double *)maxval >= pFilter->data.lowerBndd);
}

bool rangeFilter_dd_ei(SColumnFilter *pFilter, char *minval, char *maxval) {
  return (*(double *)minval <= pFilter->data.upperBndd && *(double *)maxval > pFilter->data.lowerBndd);
}

////////////////////////////////////////////////////////////////////////////
bool (*filterFunc_i8[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_i8, large_i8, equal_i8, lessEqual_i8, largeEqual_i8, nequal_i8, NULL,
};

bool (*filterFunc_i16[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_i16, large_i16, equal_i16, lessEqual_i16, largeEqual_i16, nequal_i16, NULL,
};

bool (*filterFunc_i32[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_i32, large_i32, equal_i32, lessEqual_i32, largeEqual_i32, nequal_i32, NULL,
};

bool (*filterFunc_i64[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_i64, large_i64, equal_i64, lessEqual_i64, largeEqual_i64, nequal_i64, NULL,
};

bool (*filterFunc_ds[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_ds, large_ds, equal_ds, lessEqual_ds, largeEqual_ds, nequal_ds, NULL,
};

bool (*filterFunc_dd[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, less_dd, large_dd, equal_dd, lessEqual_dd, largeEqual_dd, nequal_dd, NULL,
};

bool (*filterFunc_str[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, NULL, NULL, equal_str, NULL, NULL, nequal_str, like_str,
};

bool (*rangeFilterFunc_i8[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_i8_ee, rangeFilter_i8_ie, rangeFilter_i8_ei, rangeFilter_i8_ii,
};

bool (*rangeFilterFunc_i16[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_i16_ee, rangeFilter_i16_ie, rangeFilter_i16_ei, rangeFilter_i16_ii,
};

bool (*rangeFilterFunc_i32[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_i32_ee, rangeFilter_i32_ie, rangeFilter_i32_ei, rangeFilter_i32_ii,
};

bool (*rangeFilterFunc_i64[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_i64_ee, rangeFilter_i64_ie, rangeFilter_i64_ei, rangeFilter_i64_ii,
};

bool (*rangeFilterFunc_ds[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_ds_ee, rangeFilter_ds_ie, rangeFilter_ds_ei, rangeFilter_ds_ii,
};

bool (*rangeFilterFunc_dd[])(SColumnFilter *pFilter, char *minval, char *maxval) = {
    NULL, rangeFilter_dd_ee, rangeFilter_dd_ie, rangeFilter_dd_ei, rangeFilter_dd_ii,
};

__filter_func_t *vnodeGetRangeFilterFuncArray(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return rangeFilterFunc_i8;
    case TSDB_DATA_TYPE_TINYINT:
      return rangeFilterFunc_i8;
    case TSDB_DATA_TYPE_SMALLINT:
      return rangeFilterFunc_i16;
    case TSDB_DATA_TYPE_INT:
      return rangeFilterFunc_i32;
    case TSDB_DATA_TYPE_TIMESTAMP:  // timestamp uses bigint filter
    case TSDB_DATA_TYPE_BIGINT:
      return rangeFilterFunc_i64;
    case TSDB_DATA_TYPE_FLOAT:
      return rangeFilterFunc_ds;
    case TSDB_DATA_TYPE_DOUBLE:
      return rangeFilterFunc_dd;
    default:
      return NULL;
  }
}

__filter_func_t *vnodeGetValueFilterFuncArray(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return filterFunc_i8;
    case TSDB_DATA_TYPE_TINYINT:
      return filterFunc_i8;
    case TSDB_DATA_TYPE_SMALLINT:
      return filterFunc_i16;
    case TSDB_DATA_TYPE_INT:
      return filterFunc_i32;
    case TSDB_DATA_TYPE_TIMESTAMP:  // timestamp uses bigint filter
    case TSDB_DATA_TYPE_BIGINT:
      return filterFunc_i64;
    case TSDB_DATA_TYPE_FLOAT:
      return filterFunc_ds;
    case TSDB_DATA_TYPE_DOUBLE:
      return filterFunc_dd;
    case TSDB_DATA_TYPE_BINARY:
      return filterFunc_str;
    default:
      return NULL;
  }
}

bool vnodeSupportPrefilter(int32_t type) { return type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR; }
