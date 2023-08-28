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

#define PATTERN_COMPARE_INFO_INITIALIZER { '%', '_', L'%', L'_' }

typedef struct SPatternCompareInfo {
  char matchAll;       // symbol for match all wildcard, default: '%'
  char matchOne;       // symbol for match one wildcard, default: '_'
  TdUcs4 umatchAll;    // unicode version matchAll
  TdUcs4 umatchOne;    // unicode version matchOne
} SPatternCompareInfo;

int32_t patternMatch(const char *pattern, size_t psize, const char *str, size_t ssize, const SPatternCompareInfo *pInfo);

int32_t wcsPatternMatch(const TdUcs4 *pattern, size_t psize, const TdUcs4 *str, size_t ssize, const SPatternCompareInfo *pInfo);

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
int32_t compareLenBinaryVal(const void *pLeft, const void *pRight);

int32_t comparestrRegexMatch(const void *pLeft, const void *pRight);
int32_t comparestrRegexNMatch(const void *pLeft, const void *pRight);

int32_t comparewcsRegexMatch(const void *pLeft, const void *pRight);
int32_t comparewcsRegexNMatch(const void *pLeft, const void *pRight);

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
int32_t compareLenBinaryValDesc(const void *pLeft, const void *pRight);

int32_t comparestrPatternMatch(const void *pLeft, const void *pRight);
int32_t comparestrPatternNMatch(const void *pLeft, const void *pRight);

int32_t comparewcsPatternMatch(const void *pLeft, const void *pRight);
int32_t comparewcsPatternNMatch(const void *pLeft, const void *pRight);

int32_t compareInt8Int16(const void *pLeft, const void *pRight);
int32_t compareInt8Int32(const void *pLeft, const void *pRight);
int32_t compareInt8Int64(const void *pLeft, const void *pRight);
int32_t compareInt8Float(const void *pLeft, const void *pRight);
int32_t compareInt8Double(const void *pLeft, const void *pRight);
int32_t compareInt8Uint8(const void *pLeft, const void *pRight);
int32_t compareInt8Uint16(const void *pLeft, const void *pRight);
int32_t compareInt8Uint32(const void *pLeft, const void *pRight);
int32_t compareInt8Uint64(const void *pLeft, const void *pRight);
int32_t compareInt16Int8(const void *pLeft, const void *pRight);
int32_t compareInt16Int32(const void *pLeft, const void *pRight);
int32_t compareInt16Int64(const void *pLeft, const void *pRight);
int32_t compareInt16Float(const void *pLeft, const void *pRight);
int32_t compareInt16Double(const void *pLeft, const void *pRight);
int32_t compareInt16Uint8(const void *pLeft, const void *pRight);
int32_t compareInt16Uint16(const void *pLeft, const void *pRight);
int32_t compareInt16Uint32(const void *pLeft, const void *pRight);
int32_t compareInt16Uint64(const void *pLeft, const void *pRight);
int32_t compareInt32Int8(const void *pLeft, const void *pRight);
int32_t compareInt32Int16(const void *pLeft, const void *pRight);
int32_t compareInt32Int64(const void *pLeft, const void *pRight);
int32_t compareInt32Float(const void *pLeft, const void *pRight);
int32_t compareInt32Double(const void *pLeft, const void *pRight);
int32_t compareInt32Uint8(const void *pLeft, const void *pRight);
int32_t compareInt32Uint16(const void *pLeft, const void *pRight);
int32_t compareInt32Uint32(const void *pLeft, const void *pRight);
int32_t compareInt32Uint64(const void *pLeft, const void *pRight);
int32_t compareInt64Int8(const void *pLeft, const void *pRight);
int32_t compareInt64Int16(const void *pLeft, const void *pRight);
int32_t compareInt64Int32(const void *pLeft, const void *pRight);
int32_t compareInt64Float(const void *pLeft, const void *pRight);
int32_t compareInt64Double(const void *pLeft, const void *pRight);
int32_t compareInt64Uint8(const void *pLeft, const void *pRight);
int32_t compareInt64Uint16(const void *pLeft, const void *pRight);
int32_t compareInt64Uint32(const void *pLeft, const void *pRight);
int32_t compareInt64Uint64(const void *pLeft, const void *pRight);
int32_t compareFloatInt8(const void *pLeft, const void *pRight);
int32_t compareFloatInt16(const void *pLeft, const void *pRight);
int32_t compareFloatInt32(const void *pLeft, const void *pRight);
int32_t compareFloatInt64(const void *pLeft, const void *pRight);
int32_t compareFloatDouble(const void *pLeft, const void *pRight);
int32_t compareFloatUint8(const void *pLeft, const void *pRight);
int32_t compareFloatUint16(const void *pLeft, const void *pRight);
int32_t compareFloatUint32(const void *pLeft, const void *pRight);
int32_t compareFloatUint64(const void *pLeft, const void *pRight);
int32_t compareDoubleInt8(const void *pLeft, const void *pRight);
int32_t compareDoubleInt16(const void *pLeft, const void *pRight);
int32_t compareDoubleInt32(const void *pLeft, const void *pRight);
int32_t compareDoubleInt64(const void *pLeft, const void *pRight);
int32_t compareDoubleFloat(const void *pLeft, const void *pRight);
int32_t compareDoubleUint8(const void *pLeft, const void *pRight);
int32_t compareDoubleUint16(const void *pLeft, const void *pRight);
int32_t compareDoubleUint32(const void *pLeft, const void *pRight);
int32_t compareDoubleUint64(const void *pLeft, const void *pRight);
int32_t compareUint8Int8(const void *pLeft, const void *pRight);
int32_t compareUint8Int16(const void *pLeft, const void *pRight);
int32_t compareUint8Int32(const void *pLeft, const void *pRight);
int32_t compareUint8Int64(const void *pLeft, const void *pRight);
int32_t compareUint8Float(const void *pLeft, const void *pRight);
int32_t compareUint8Double(const void *pLeft, const void *pRight);
int32_t compareUint8Uint16(const void *pLeft, const void *pRight);
int32_t compareUint8Uint32(const void *pLeft, const void *pRight);
int32_t compareUint8Uint64(const void *pLeft, const void *pRight);
int32_t compareUint16Int8(const void *pLeft, const void *pRight);
int32_t compareUint16Int16(const void *pLeft, const void *pRight);
int32_t compareUint16Int32(const void *pLeft, const void *pRight);
int32_t compareUint16Int64(const void *pLeft, const void *pRight);
int32_t compareUint16Float(const void *pLeft, const void *pRight);
int32_t compareUint16Double(const void *pLeft, const void *pRight);
int32_t compareUint16Uint8(const void *pLeft, const void *pRight);
int32_t compareUint16Uint32(const void *pLeft, const void *pRight);
int32_t compareUint16Uint64(const void *pLeft, const void *pRight);
int32_t compareUint32Int8(const void *pLeft, const void *pRight);
int32_t compareUint32Int16(const void *pLeft, const void *pRight);
int32_t compareUint32Int32(const void *pLeft, const void *pRight);
int32_t compareUint32Int64(const void *pLeft, const void *pRight);
int32_t compareUint32Float(const void *pLeft, const void *pRight);
int32_t compareUint32Double(const void *pLeft, const void *pRight);
int32_t compareUint32Uint8(const void *pLeft, const void *pRight);
int32_t compareUint32Uint16(const void *pLeft, const void *pRight);
int32_t compareUint32Uint64(const void *pLeft, const void *pRight);
int32_t compareUint64Int8(const void *pLeft, const void *pRight);
int32_t compareUint64Int16(const void *pLeft, const void *pRight);
int32_t compareUint64Int32(const void *pLeft, const void *pRight);
int32_t compareUint64Int64(const void *pLeft, const void *pRight);
int32_t compareUint64Float(const void *pLeft, const void *pRight);
int32_t compareUint64Double(const void *pLeft, const void *pRight);
int32_t compareUint64Uint8(const void *pLeft, const void *pRight);
int32_t compareUint64Uint16(const void *pLeft, const void *pRight);
int32_t compareUint64Uint32(const void *pLeft, const void *pRight);

__compar_fn_t getComparFunc(int32_t type, int32_t optr);
__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPARE_H_*/
