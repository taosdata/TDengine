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

#ifndef _TD_UTIL_COMPARE_H_
#define _TD_UTIL_COMPARE_H_

#include "os.h"
#include "taos.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_PATTERN_MATCH              0
#define TSDB_PATTERN_NOMATCH            1
#define TSDB_PATTERN_NOWILDCARDMATCH    2
#define TSDB_PATTERN_STRING_DEFAULT_LEN 100
#define TSDB_REGEX_STRING_DEFAULT_LEN   128

#define FLT_COMPAR_TOL_FACTOR    4
#define FLT_EQUAL(_x, _y)        (fabs((_x) - (_y)) <= (FLT_COMPAR_TOL_FACTOR * FLT_EPSILON))
#define FLT_GREATER(_x, _y)      (!FLT_EQUAL((_x), (_y)) && ((_x) > (_y)))
#define FLT_LESS(_x, _y)         (!FLT_EQUAL((_x), (_y)) && ((_x) < (_y)))
#define FLT_GREATEREQUAL(_x, _y) (FLT_EQUAL((_x), (_y)) || ((_x) > (_y)))
#define FLT_LESSEQUAL(_x, _y)    (FLT_EQUAL((_x), (_y)) || ((_x) < (_y)))

#define PATTERN_COMPARE_INFO_INITIALIZER \
  { '%', '_' }

typedef struct SPatternCompareInfo {
  char matchAll;  // symbol for match all wildcard, default: '%'
  char matchOne;  // symbol for match one wildcard, default: '_'
} SPatternCompareInfo;

int32_t patternMatch(const char *pattern, const char *str, size_t size, const SPatternCompareInfo *pInfo);

int32_t WCSPatternMatch(const TdUcs4 *pattern, const TdUcs4 *str, size_t size, const SPatternCompareInfo *pInfo);

int32_t taosArrayCompareString(const void *a, const void *b);

int32_t setChkInBytes1(const void *pLeft, const void *pRight);
int32_t setChkInBytes2(const void *pLeft, const void *pRight);
int32_t setChkInBytes4(const void *pLeft, const void *pRight);
int32_t setChkInBytes8(const void *pLeft, const void *pRight);

int32_t setChkNotInBytes1(const void *pLeft, const void *pRight);
int32_t setChkNotInBytes2(const void *pLeft, const void *pRight);
int32_t setChkNotInBytes4(const void *pLeft, const void *pRight);
int32_t setChkNotInBytes8(const void *pLeft, const void *pRight);

int32_t compareChkInString(const void *pLeft, const void *pRight);
int32_t compareChkNotInString(const void *pLeft, const void *pRight);

int32_t compareInt8Val(const void *pLeft, const void *pRight);
int32_t compareInt16Val(const void *pLeft, const void *pRight);
int32_t compareInt32Val(const void *pLeft, const void *pRight);
int32_t compareInt64Val(const void *pLeft, const void *pRight);

int32_t compareUint8Val(const void *pLeft, const void *pRight);
int32_t compareUint16Val(const void *pLeft, const void *pRight);
int32_t compareUint32Val(const void *pLeft, const void *pRight);
int32_t compareUint64Val(const void *pLeft, const void *pRight);

int32_t compareFloatVal(const void *pLeft, const void *pRight);
int32_t compareDoubleVal(const void *pLeft, const void *pRight);

int32_t compareLenPrefixedStr(const void *pLeft, const void *pRight);
int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight);

int32_t compareStrRegexComp(const void *pLeft, const void *pRight);
int32_t compareStrRegexCompMatch(const void *pLeft, const void *pRight);
int32_t compareStrRegexCompNMatch(const void *pLeft, const void *pRight);

int32_t compareInt8ValDesc(const void *pLeft, const void *pRight);
int32_t compareInt16ValDesc(const void *pLeft, const void *pRight);
int32_t compareInt32ValDesc(const void *pLeft, const void *pRight);
int32_t compareInt64ValDesc(const void *pLeft, const void *pRight);

int32_t compareFloatValDesc(const void *pLeft, const void *pRight);
int32_t compareDoubleValDesc(const void *pLeft, const void *pRight);

int32_t compareUint8ValDesc(const void *pLeft, const void *pRight);
int32_t compareUint16ValDesc(const void *pLeft, const void *pRight);
int32_t compareUint32ValDesc(const void *pLeft, const void *pRight);
int32_t compareUint64ValDesc(const void *pLeft, const void *pRight);

int32_t compareLenPrefixedStrDesc(const void *pLeft, const void *pRight);
int32_t compareLenPrefixedWStrDesc(const void *pLeft, const void *pRight);

int32_t compareStrPatternMatch(const void *pLeft, const void *pRight);
int32_t compareStrPatternNotMatch(const void *pLeft, const void *pRight);

int32_t compareWStrPatternMatch(const void *pLeft, const void *pRight);
int32_t compareWStrPatternNotMatch(const void *pLeft, const void *pRight);

__compar_fn_t getComparFunc(int32_t type, int32_t optr);
__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order);
int32_t       doCompare(const char *a, const char *b, int32_t type, size_t size);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPARE_H_*/
