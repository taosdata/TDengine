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

#define TSDB_PATTERN_MATCH 0
#define TSDB_PATTERN_NOMATCH 1
#define TSDB_PATTERN_NOWILDCARDMATCH 2
#define TSDB_PATTERN_STRING_MAX_LEN 20

#define PATTERN_COMPARE_INFO_INITIALIZER { '%', '_' }

typedef struct SPatternCompareInfo {
  char matchAll;  // symbol for match all wildcard, default: '%'
  char matchOne;  // symbol for match one wildcard, default: '_'
} SPatternCompareInfo;

int patternMatch(const char *zPattern, const char *zString, size_t size, const SPatternCompareInfo *pInfo);

int WCSPatternMatch(const wchar_t *zPattern, const wchar_t *zString, size_t size, const SPatternCompareInfo *pInfo);

int32_t doCompare(const char* f1, const char* f2, int32_t type, size_t size);

__compar_fn_t getKeyComparFunc(int32_t keyType);

__compar_fn_t getComparFunc(int32_t type, int32_t optr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCOMPARE_H
