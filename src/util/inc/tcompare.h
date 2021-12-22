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

#ifndef TDENGINE_TCOMPARE_H
#define TDENGINE_TCOMPARE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#define TSDB_PATTERN_MATCH               0
#define TSDB_PATTERN_NOMATCH             1
#define TSDB_PATTERN_NOWILDCARDMATCH     2
#define TSDB_PATTERN_STRING_DEFAULT_LEN  100

#define FLT_COMPAR_TOL_FACTOR    4
#define FLT_EQUAL(_x, _y)        (fabs((_x) - (_y)) <= (FLT_COMPAR_TOL_FACTOR * FLT_EPSILON))
#define FLT_GREATER(_x, _y)      (!FLT_EQUAL((_x), (_y)) && ((_x) > (_y)))
#define FLT_LESS(_x, _y)         (!FLT_EQUAL((_x), (_y)) && ((_x) < (_y)))
#define FLT_GREATEREQUAL(_x, _y) (FLT_EQUAL((_x), (_y)) || ((_x) > (_y)))
#define FLT_LESSEQUAL(_x, _y)    (FLT_EQUAL((_x), (_y)) || ((_x) < (_y)))

#define PATTERN_COMPARE_INFO_INITIALIZER { '%', '_' }

typedef struct SPatternCompareInfo {
  char matchAll;  // symbol for match all wildcard, default: '%'
  char matchOne;  // symbol for match one wildcard, default: '_'
} SPatternCompareInfo;

int patternMatch(const char *pattern, const char *str, size_t size, const SPatternCompareInfo *pInfo);

int WCSPatternMatch(const uint32_t *pattern, const uint32_t *str, size_t size, const SPatternCompareInfo *pInfo);

int32_t doCompare(const char* a, const char* b, int32_t type, size_t size);

__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order);

__compar_fn_t getComparFunc(int32_t type, int32_t optr);

int32_t taosArrayCompareString(const void* a, const void* b);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCOMPARE_H
