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
#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE
#include "tcompare.h"
#include "regex.h"
#include "tdef.h"
#include "thash.h"
#include "tlog.h"
#include "types.h"

int32_t setChkInBytes1(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 1) ? 1 : 0;
}

int32_t setChkInBytes2(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 2) ? 1 : 0;
}

int32_t setChkInBytes4(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 4) ? 1 : 0;
}

int32_t setChkInBytes8(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 8) ? 1 : 0;
}

int32_t setChkNotInBytes1(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, 1) ? 1 : 0;
}

int32_t setChkNotInBytes2(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, 2) ? 1 : 0;
}

int32_t setChkNotInBytes4(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, 4) ? 1 : 0;
}

int32_t setChkNotInBytes8(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, 8) ? 1 : 0;
}

int32_t compareChkInString(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, varDataVal(pLeft), varDataLen(pLeft)) ? 1 : 0;
}

int32_t compareChkNotInString(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, varDataVal(pLeft), varDataLen(pLeft)) ? 1 : 0;
}

int32_t compareInt8Val(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft), right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8ValDesc(const void *pLeft, const void *pRight) { return compareInt8Val(pRight, pLeft); }

int32_t compareInt16Val(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft), right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16ValDesc(const void *pLeft, const void *pRight) { return compareInt16Val(pRight, pLeft); }

int32_t compareInt32Val(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft), right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32ValDesc(const void *pLeft, const void *pRight) { return compareInt32Val(pRight, pLeft); }

int32_t compareInt64Val(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft), right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64ValDesc(const void *pLeft, const void *pRight) { return compareInt64Val(pRight, pLeft); }

int32_t compareUint32Val(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft), right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32ValDesc(const void *pLeft, const void *pRight) { return compareUint32Val(pRight, pLeft); }

int32_t compareUint64Val(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft), right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64ValDesc(const void *pLeft, const void *pRight) { return compareUint64Val(pRight, pLeft); }

int32_t compareUint16Val(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft), right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16ValDesc(const void *pLeft, const void *pRight) { return compareUint16Val(pRight, pLeft); }

int32_t compareUint8Val(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft), right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8ValDesc(const void *pLeft, const void *pRight) { return compareUint8Val(pRight, pLeft); }

int32_t compareFloatVal(const void *pLeft, const void *pRight) {
  float p1 = GET_FLOAT_VAL(pLeft);
  float p2 = GET_FLOAT_VAL(pRight);

  if (isnan(p1) && isnan(p2)) {
    return 0;
  }

  if (isnan(p1)) {
    return -1;
  }

  if (isnan(p2)) {
    return 1;
  }
  if (FLT_EQUAL(p1, p2)) {
    return 0;
  }
  return FLT_GREATER(p1, p2) ? 1 : -1;
}

int32_t compareFloatValDesc(const void *pLeft, const void *pRight) { return compareFloatVal(pRight, pLeft); }

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double p1 = GET_DOUBLE_VAL(pLeft);
  double p2 = GET_DOUBLE_VAL(pRight);

  if (isnan(p1) && isnan(p2)) {
    return 0;
  }

  if (isnan(p1)) {
    return -1;
  }

  if (isnan(p2)) {
    return 1;
  }

  if (FLT_EQUAL(p1, p2)) {
    return 0;
  }
  return FLT_GREATER(p1, p2) ? 1 : -1;
}

int32_t compareDoubleValDesc(const void *pLeft, const void *pRight) { return compareDoubleVal(pRight, pLeft); }

int32_t compareLenPrefixedStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  if (len1 != len2) {
    return len1 > len2 ? 1 : -1;
  } else {
    int32_t ret = strncmp(varDataVal(pLeft), varDataVal(pRight), len1);
    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1 : -1;
    }
  }
}

int32_t compareLenPrefixedStrDesc(const void *pLeft, const void *pRight) {
  return compareLenPrefixedStr(pRight, pLeft);
}

int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  if (len1 != len2) {
    return len1 > len2 ? 1 : -1;
  } else {
    int32_t ret = memcmp((TdUcs4 *)pLeft, (TdUcs4 *)pRight, len1);
    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1 : -1;
    }
  }
}

int32_t compareLenPrefixedWStrDesc(const void *pLeft, const void *pRight) {
  return compareLenPrefixedWStr(pRight, pLeft);
}

/*
 * Compare two strings
 *    TSDB_MATCH:            Match
 *    TSDB_NOMATCH:          No match
 *    TSDB_NOWILDCARDMATCH:  No match in spite of having * or % wildcards.
 * Like matching rules:
 *      '%': Matches zero or more characters
 *      '_': Matches one character
 *
 */
int32_t patternMatch(const char *patterStr, const char *str, size_t size, const SPatternCompareInfo *pInfo) {
  char c, c1;

  int32_t i = 0;
  int32_t j = 0;
  int32_t o = 0;
  int32_t m = 0;

  while ((c = patterStr[i++]) != 0) {
    if (c == pInfo->matchAll) { /* Match "*" */

      while ((c = patterStr[i++]) == pInfo->matchAll || c == pInfo->matchOne) {
        if (c == pInfo->matchOne) {
          if (j > size || str[j++] == 0) {
            // empty string, return not match
            return TSDB_PATTERN_NOWILDCARDMATCH;
          } else {
            ++o;
          }
        }
      }

      if (c == 0) {
        return TSDB_PATTERN_MATCH; /* "*" at the end of the pattern matches */
      }

      char next[3] = {toupper(c), tolower(c), 0};
      m = o;
      while (1) {
        size_t n = strcspn(str + m, next);
        str += m + n;

        if (str[0] == 0 || (n >= size)) {
          break;
        }

        int32_t ret = patternMatch(&patterStr[i], ++str, size - n - 1, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
        m = 0;
      }
      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    c1 = str[j++];
    ++o;

    if (j <= size) {
      if (c == '\\' && patterStr[i] == '_' && c1 == '_') {
        i++;
        continue;
      }
      if (c == c1 || tolower(c) == tolower(c1) || (c == pInfo->matchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int32_t WCSPatternMatch(const TdUcs4 *patterStr, const TdUcs4 *str, size_t size, const SPatternCompareInfo *pInfo) {
  TdUcs4 c, c1;
  TdUcs4 matchOne = L'_';  // "_"
  TdUcs4 matchAll = L'%';  // "%"

  int32_t i = 0;
  int32_t j = 0;

  while ((c = patterStr[i++]) != 0) {
    if (c == matchAll) { /* Match "%" */

      while ((c = patterStr[i++]) == matchAll || c == matchOne) {
        if (c == matchOne && (j >= size || str[j++] == 0)) {
          return TSDB_PATTERN_NOWILDCARDMATCH;
        }
      }
      if (c == 0) {
        return TSDB_PATTERN_MATCH;
      }

      TdUcs4 accept[3] = {towupper(c), towlower(c), 0};
      while (1) {
        size_t n = wcscspn(str, accept);

        str += n;
        if (str[0] == 0 || (n >= size)) {
          break;
        }

        int32_t ret = WCSPatternMatch(&patterStr[i], ++str, size - n - 1, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    c1 = str[j++];

    if (j <= size) {
      if (c == c1 || towlower(c) == towlower(c1) || (c == matchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int32_t compareStrRegexCompMatch(const void *pLeft, const void *pRight) { return compareStrRegexComp(pLeft, pRight); }

int32_t compareStrRegexCompNMatch(const void *pLeft, const void *pRight) {
  return compareStrRegexComp(pLeft, pRight) ? 0 : 1;
}

int32_t compareStrRegexComp(const void *pLeft, const void *pRight) {
  size_t sz = varDataLen(pRight);
  char  *pattern = taosMemoryMalloc(sz + 1);
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));
  pattern[sz] = 0;

  sz = varDataLen(pLeft);
  char *str = taosMemoryMalloc(sz + 1);
  memcpy(str, varDataVal(pLeft), sz);
  str[sz] = 0;

  int32_t errCode = 0;
  regex_t regex;
  char    msgbuf[256] = {0};

  int32_t cflags = REG_EXTENDED;
  if ((errCode = regcomp(&regex, pattern, cflags)) != 0) {
    regerror(errCode, &regex, msgbuf, sizeof(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pattern, msgbuf);
    regfree(&regex);
    taosMemoryFree(str);
    taosMemoryFree(pattern);
    return 1;
  }

  errCode = regexec(&regex, str, 0, NULL, 0);
  if (errCode != 0 && errCode != REG_NOMATCH) {
    regerror(errCode, &regex, msgbuf, sizeof(msgbuf));
    uDebug("Failed to match %s with pattern %s, reason %s", str, pattern, msgbuf)
  }
  int32_t result = (errCode == 0) ? 0 : 1;
  regfree(&regex);
  taosMemoryFree(str);
  taosMemoryFree(pattern);
  return result;
}

int32_t taosArrayCompareString(const void *a, const void *b) {
  const char *x = *(const char **)a;
  const char *y = *(const char **)b;

  return compareLenPrefixedStr(x, y);
}

int32_t compareStrPatternMatch(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  assert(varDataLen(pRight) <= TSDB_MAX_FIELD_LEN);
  char *pattern = taosMemoryCalloc(varDataLen(pRight) + 1, sizeof(char));
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));

  size_t sz = varDataLen(pLeft);
  char  *buf = taosMemoryMalloc(sz + 1);
  memcpy(buf, varDataVal(pLeft), sz);
  buf[sz] = 0;

  int32_t ret = patternMatch(pattern, buf, sz, &pInfo);
  taosMemoryFree(buf);
  taosMemoryFree(pattern);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t compareStrPatternNotMatch(const void *pLeft, const void *pRight) {
  return compareStrPatternMatch(pLeft, pRight) ? 0 : 1;
}

int32_t compareWStrPatternMatch(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  assert(varDataLen(pRight) <= TSDB_MAX_FIELD_LEN * TSDB_NCHAR_SIZE);

  char *pattern = taosMemoryCalloc(varDataLen(pRight) + TSDB_NCHAR_SIZE, 1);
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));

  int32_t ret = WCSPatternMatch((TdUcs4*)pattern, (TdUcs4*)varDataVal(pLeft), varDataLen(pLeft) / TSDB_NCHAR_SIZE, &pInfo);
  taosMemoryFree(pattern);

  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t compareWStrPatternNotMatch(const void *pLeft, const void *pRight) {
  return compareWStrPatternMatch(pLeft, pRight) ? 0 : 1;
}
__compar_fn_t getComparFunc(int32_t type, int32_t optr) {
  __compar_fn_t comparFn = NULL;

  if (optr == OP_TYPE_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
        return setChkInBytes1;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        return setChkInBytes2;
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_FLOAT:
        return setChkInBytes4;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
        return setChkInBytes8;
      default:
        assert(0);
    }
  }

  if (optr == OP_TYPE_NOT_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
        return setChkNotInBytes1;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        return setChkNotInBytes2;
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_FLOAT:
        return setChkNotInBytes4;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
        return setChkNotInBytes8;
      default:
        assert(0);
    }
  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      comparFn = compareInt8Val;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      comparFn = compareInt16Val;
      break;
    case TSDB_DATA_TYPE_INT:
      comparFn = compareInt32Val;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      comparFn = compareInt64Val;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      comparFn = compareFloatVal;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = compareDoubleVal;
      break;
    case TSDB_DATA_TYPE_BINARY: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = compareStrRegexCompMatch;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = compareStrRegexCompNMatch;
      } else if (optr == OP_TYPE_LIKE) { /* wildcard query using like operator */
        comparFn = compareStrPatternMatch;
      } else if (optr == OP_TYPE_NOT_LIKE) { /* wildcard query using like operator */
        comparFn = compareStrPatternNotMatch;
      } else if (optr == OP_TYPE_IN) {
        comparFn = compareChkInString;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = compareChkNotInString;
      } else { /* normal relational comparFn */
        comparFn = compareLenPrefixedStr;
      }

      break;
    }

    case TSDB_DATA_TYPE_NCHAR: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = compareStrRegexCompMatch;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = compareStrRegexCompNMatch;
      } else if (optr == OP_TYPE_LIKE) {
        comparFn = compareWStrPatternMatch;
      } else if (optr == OP_TYPE_NOT_LIKE) {
        comparFn = compareWStrPatternNotMatch;
      } else if (optr == OP_TYPE_IN) {
        comparFn = compareChkInString;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = compareChkNotInString;
      } else {
        comparFn = compareLenPrefixedWStr;
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT:
      comparFn = compareUint8Val;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      comparFn = compareUint16Val;
      break;
    case TSDB_DATA_TYPE_UINT:
      comparFn = compareUint32Val;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      comparFn = compareUint64Val;
      break;

    default:
      comparFn = compareInt32Val;
      break;
  }

  return comparFn;
}

__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order) {
  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
      return (order == TSDB_ORDER_ASC) ? compareInt8Val : compareInt8ValDesc;
    case TSDB_DATA_TYPE_SMALLINT:
      return (order == TSDB_ORDER_ASC) ? compareInt16Val : compareInt16ValDesc;
    case TSDB_DATA_TYPE_INT:
      return (order == TSDB_ORDER_ASC) ? compareInt32Val : compareInt32ValDesc;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return (order == TSDB_ORDER_ASC) ? compareInt64Val : compareInt64ValDesc;
    case TSDB_DATA_TYPE_FLOAT:
      return (order == TSDB_ORDER_ASC) ? compareFloatVal : compareFloatValDesc;
    case TSDB_DATA_TYPE_DOUBLE:
      return (order == TSDB_ORDER_ASC) ? compareDoubleVal : compareDoubleValDesc;
    case TSDB_DATA_TYPE_UTINYINT:
      return (order == TSDB_ORDER_ASC) ? compareUint8Val : compareUint8ValDesc;
    case TSDB_DATA_TYPE_USMALLINT:
      return (order == TSDB_ORDER_ASC) ? compareUint16Val : compareUint16ValDesc;
    case TSDB_DATA_TYPE_UINT:
      return (order == TSDB_ORDER_ASC) ? compareUint32Val : compareUint32ValDesc;
    case TSDB_DATA_TYPE_UBIGINT:
      return (order == TSDB_ORDER_ASC) ? compareUint64Val : compareUint64ValDesc;
    case TSDB_DATA_TYPE_BINARY:
      return (order == TSDB_ORDER_ASC) ? compareLenPrefixedStr : compareLenPrefixedStrDesc;
    case TSDB_DATA_TYPE_NCHAR:
      return (order == TSDB_ORDER_ASC) ? compareLenPrefixedWStr : compareLenPrefixedWStrDesc;
    default:
      return (order == TSDB_ORDER_ASC) ? compareInt32Val : compareInt32ValDesc;
  }
}

int32_t doCompare(const char *f1, const char *f2, int32_t type, size_t size) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      DEFAULT_COMP(GET_INT32_VAL(f1), GET_INT32_VAL(f2));
    case TSDB_DATA_TYPE_DOUBLE:
      DEFAULT_DOUBLE_COMP(GET_DOUBLE_VAL(f1), GET_DOUBLE_VAL(f2));
    case TSDB_DATA_TYPE_FLOAT:
      DEFAULT_FLOAT_COMP(GET_FLOAT_VAL(f1), GET_FLOAT_VAL(f2));
    case TSDB_DATA_TYPE_BIGINT:
      DEFAULT_COMP(GET_INT64_VAL(f1), GET_INT64_VAL(f2));
    case TSDB_DATA_TYPE_SMALLINT:
      DEFAULT_COMP(GET_INT16_VAL(f1), GET_INT16_VAL(f2));
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
      DEFAULT_COMP(GET_INT8_VAL(f1), GET_INT8_VAL(f2));
    case TSDB_DATA_TYPE_UTINYINT:
      DEFAULT_COMP(GET_UINT8_VAL(f1), GET_UINT8_VAL(f2));
    case TSDB_DATA_TYPE_USMALLINT:
      DEFAULT_COMP(GET_UINT16_VAL(f1), GET_UINT16_VAL(f2));
    case TSDB_DATA_TYPE_UINT:
      DEFAULT_COMP(GET_UINT32_VAL(f1), GET_UINT32_VAL(f2));
    case TSDB_DATA_TYPE_UBIGINT:
      DEFAULT_COMP(GET_UINT64_VAL(f1), GET_UINT64_VAL(f2));
    case TSDB_DATA_TYPE_NCHAR: {
      tstr *t1 = (tstr *)f1;
      tstr *t2 = (tstr *)f2;

      if (t1->len != t2->len) {
        return t1->len > t2->len ? 1 : -1;
      }
      int32_t ret = memcmp((TdUcs4 *)t1, (TdUcs4 *)t2, t2->len);
      if (ret == 0) {
        return ret;
      }
      return (ret < 0) ? -1 : 1;
    }
    default: {  // todo refactor
      tstr *t1 = (tstr *)f1;
      tstr *t2 = (tstr *)f2;

      if (t1->len != t2->len) {
        return t1->len > t2->len ? 1 : -1;
      } else {
        int32_t ret = strncmp(t1->data, t2->data, t1->len);
        if (ret == 0) {
          return 0;
        } else {
          return ret < 0 ? -1 : 1;
        }
      }
    }
  }
}
