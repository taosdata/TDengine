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

#include "os.h"
#include "types.h"
#include "compare.h"
#include "ulog.h"
#include "thash.h"
#include "regex.h"

int32_t setCompareBytes1(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 1) ? 1 : 0;
}

int32_t setCompareBytes2(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 2) ? 1 : 0;
}

int32_t setCompareBytes4(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 4) ? 1 : 0;
}

int32_t setCompareBytes8(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, 8) ? 1 : 0;
}

int32_t compareInt8Val(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft), right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8ValDesc(const void *pLeft, const void *pRight) {
  return compareInt8Val(pRight, pLeft);
}

int32_t compareInt16Val(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft), right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16ValDesc(const void* pLeft, const void* pRight) {
  return compareInt16Val(pRight, pLeft);
}

int32_t compareInt32Val(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft), right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32ValDesc(const void* pLeft, const void* pRight) {
  return compareInt32Val(pRight, pLeft);
}

int32_t compareInt64Val(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft), right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64ValDesc(const void* pLeft, const void* pRight) {
  return compareInt64Val(pRight, pLeft);
}

int32_t compareUint32Val(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft), right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32ValDesc(const void* pLeft, const void* pRight) {
  return compareUint32Val(pRight, pLeft);
}

int32_t compareUint64Val(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft), right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64ValDesc(const void* pLeft, const void* pRight) {
  return compareUint64Val(pRight, pLeft);
}

int32_t compareUint16Val(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft), right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16ValDesc(const void* pLeft, const void* pRight) {
  return compareUint16Val(pRight, pLeft);
}

int32_t compareUint8Val(const void* pLeft, const void* pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft), right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8ValDesc(const void* pLeft, const void* pRight) {
  return compareUint8Val(pRight, pLeft);
}

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
  return FLT_GREATER(p1, p2) ? 1: -1;
}

int32_t compareFloatValDesc(const void* pLeft, const void* pRight) {
  return compareFloatVal(pRight, pLeft);
}

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
  return FLT_GREATER(p1, p2) ? 1: -1;
}

int32_t compareDoubleValDesc(const void* pLeft, const void* pRight) {
  return compareDoubleVal(pRight, pLeft);
}

int32_t compareLenPrefixedStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  if (len1 != len2) {
    return len1 > len2? 1:-1;
  } else {
    int32_t ret = strncmp(varDataVal(pLeft), varDataVal(pRight), len1);
    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1:-1;
    }
  }
}

int32_t compareLenPrefixedStrDesc(const void* pLeft, const void* pRight) {
  return compareLenPrefixedStr(pRight, pLeft);
}

int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  if (len1 != len2) {
    return len1 > len2? 1:-1;
  } else {
    int32_t ret = memcmp((wchar_t*) pLeft, (wchar_t*) pRight, len1);
    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1 : -1;
    }
  }
}

int32_t compareLenPrefixedWStrDesc(const void* pLeft, const void* pRight) {
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
int patternMatch(const char *patterStr, const char *str, size_t size, const SPatternCompareInfo *pInfo) {
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
      if (c == '\\' && patterStr[i] == '_' && c1 == '_') { i++; continue; }
      if (c == c1 || tolower(c) == tolower(c1) || (c == pInfo->matchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int WCSPatternMatch(const wchar_t *patterStr, const wchar_t *str, size_t size, const SPatternCompareInfo *pInfo) {
  wchar_t c, c1;
  wchar_t matchOne = L'_';  // "_"
  wchar_t matchAll = L'%';  // "%"

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

      wchar_t accept[3] = {towupper(c), towlower(c), 0};
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

int32_t compareStrRegexCompMatch(const void* pLeft, const void* pRight) {
  return compareStrRegexComp(pLeft, pRight);
}

int32_t compareStrRegexCompNMatch(const void* pLeft, const void* pRight) {
  return compareStrRegexComp(pLeft, pRight) ? 0 : 1;
}

int32_t compareStrRegexComp(const void* pLeft, const void* pRight) {
  size_t sz = varDataLen(pRight);
  char *pattern = malloc(sz + 1);
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));
  pattern[sz] = 0;

  sz = varDataLen(pLeft);
  char *str = malloc(sz + 1);
  memcpy(str, varDataVal(pLeft), sz);
  str[sz] = 0;

  int errCode = 0;
  regex_t regex;
  char    msgbuf[256] = {0};

  int cflags = REG_EXTENDED;
  if ((errCode = regcomp(&regex, pattern, cflags)) != 0) {
    regerror(errCode, &regex, msgbuf, sizeof(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pattern, msgbuf);
    regfree(&regex);
    free(str);
    free(pattern);
    return 1;
  }

  errCode = regexec(&regex, str, 0, NULL, 0);
  if (errCode != 0 && errCode != REG_NOMATCH) {
    regerror(errCode, &regex, msgbuf, sizeof(msgbuf));
    uDebug("Failed to match %s with pattern %s, reason %s", str, pattern, msgbuf)
  }
  int32_t result = (errCode == 0) ? 0 : 1;
  regfree(&regex);
  free(str);
  free(pattern);
  return result;
}

int32_t taosArrayCompareString(const void* a, const void* b) {
  const char* x = *(const char**)a;
  const char* y = *(const char**)b;

  return compareLenPrefixedStr(x, y);
}

int32_t compareFindItemInSet(const void *pLeft, const void* pRight)  {
  return NULL != taosHashGet((SHashObj *)pRight, varDataVal(pLeft), varDataLen(pLeft)) ? 1 : 0;
}
