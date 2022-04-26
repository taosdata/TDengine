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
#include "tvariant.h"
#include "hash.h"
#include "os.h"
#include "regex.h"
#include "ttype.h"
#include "tulog.h"

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

  int32_t ret = strncmp(varDataVal(pLeft), varDataVal(pRight), len1>len2 ? len2:len1);
  if (ret == 0) {
    if (len1 > len2)
      return 1;
    else if(len1 < len2)
      return -1;
    else   
      return 0;
  }
  return (ret < 0) ? -1 : 1;
}

int32_t compareLenPrefixedStrDesc(const void* pLeft, const void* pRight) {
  return compareLenPrefixedStr(pRight, pLeft);
}

int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  int32_t ret = memcmp(varDataVal(pLeft), varDataVal(pRight), len1>len2 ? len2:len1);
  if (ret == 0) {
    if (len1 > len2)
      return 1;
    else if(len1 < len2)
      return -1;
    else   
      return 0;
  }
  return (ret < 0) ? -1 : 1;
}

int32_t compareLenPrefixedWStrDesc(const void* pLeft, const void* pRight) {
  return compareLenPrefixedWStr(pRight, pLeft);
}

int32_t compareJsonVal(const void *pLeft, const void *pRight) {
  const tVariant* right = pRight;
  if(right->nType != *(char*)pLeft && !(IS_NUMERIC_TYPE(right->nType) && IS_NUMERIC_TYPE(*(char*)pLeft)))
    return TSDB_DATA_JSON_CAN_NOT_COMPARE;

  uint8_t type = *(char*)pLeft;
  char* realData = POINTER_SHIFT(pLeft, CHAR_BYTES);
  if(type == TSDB_DATA_TYPE_BOOL) {
    DEFAULT_COMP(GET_INT8_VAL(realData), right->i64);
  }else if(type == TSDB_DATA_TYPE_BIGINT){
    DEFAULT_COMP(GET_INT64_VAL(realData), (right->nType == TSDB_DATA_TYPE_BIGINT) ? right->i64 : right->dKey);
  }else if(type == TSDB_DATA_TYPE_DOUBLE){
    DEFAULT_DOUBLE_COMP(GET_DOUBLE_VAL(realData), (right->nType == TSDB_DATA_TYPE_DOUBLE) ? right->dKey : right->i64);
  }else if(type == TSDB_DATA_TYPE_NCHAR){
    if (varDataLen(realData) != right->nLen) {
      return varDataLen(realData) > right->nLen ? 1 : -1;
    }
    int32_t ret = memcmp(varDataVal(realData), right->pz, right->nLen);
    if (ret == 0) {
      return ret;
    }
    return (ret < 0) ? -1 : 1;
  }else if(type == TSDB_DATA_TYPE_BINARY) { //json null
    return 0;
  }else{
    assert(0);
  }
  return 0;
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
  char escape  = '\\';  // "\"

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
      if (c == escape && patterStr[i] == pInfo->matchOne){
        if(c1 == pInfo->matchOne){
          i++;
          continue;
        }
        else{
          return TSDB_PATTERN_NOMATCH;
        }
      }
      if (c == escape && patterStr[i] == pInfo->matchAll){
        if(c1 == pInfo->matchAll){
          i++;
          continue;
        }
        else{
          return TSDB_PATTERN_NOMATCH;
        }
      }
      if (c == c1 || tolower(c) == tolower(c1) || (c == pInfo->matchOne && c1 != 0)) {
        continue;
      }
    }


    return TSDB_PATTERN_NOMATCH;
  }

  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

static uint32_t *
taosWcschr (const uint32_t *wcs, const uint32_t wc)
{
  const uint32_t *wcs2 = wcs + 1;
  if (*wcs == wc)
    return (uint32_t *) wcs;
  if (*wcs == L'\0')
    return NULL;
  do
    {
      wcs += 2;
      if (*wcs2 == wc)
        return (uint32_t *) wcs2;
      if (*wcs2 == L'\0')
        return NULL;
       wcs2 += 2;
      if (*wcs == wc)
        return (uint32_t *) wcs;
      if (*wcs == L'\0')
        return NULL;
      wcs += 2;
      if (*wcs2 == wc)
        return (uint32_t *) wcs2;
      if (*wcs2 == L'\0')
        return NULL;
      wcs2 += 2;
      if (*wcs == wc)
        return (uint32_t *) wcs;
      if (*wcs == L'\0')
        return NULL;
      wcs += 2;
      if (*wcs2 == wc)
        return (uint32_t *) wcs2;
      if (*wcs2 == L'\0')
        return NULL;
      wcs2 += 2;
      if (*wcs == wc)
        return (uint32_t *) wcs;
      if (*wcs == L'\0')
        return NULL;
      wcs += 2;
      if (*wcs2 == wc)
        return (uint32_t *) wcs2;
      if (*wcs2 == L'\0')
        return NULL;
      wcs2 += 2;
      if (*wcs == wc)
        return (uint32_t *) wcs;
    }
  while (*wcs != L'\0');
  return NULL;
}

static size_t
taosWcscspn (const uint32_t *wcs, const uint32_t *reject)
{
  size_t count = 0;
  while (*wcs != L'\0')
    if (taosWcschr (reject, *wcs++) == NULL)
      ++count;
    else
      return count;
  return count;
}

int WCSPatternMatch(const uint32_t *patterStr, const uint32_t *str, size_t size, const SPatternCompareInfo *pInfo) {
  uint32_t c, c1;
  uint32_t matchOne = (uint32_t) L'_';  // "_"
  uint32_t matchAll = (uint32_t) L'%';  // "%"
  uint32_t escape   = (uint32_t) L'\\';  // "\"

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

      uint32_t accept[3] = {towupper(c), towlower(c), 0};
      while (1) {
        size_t n = taosWcscspn(str, accept);

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
      if (c == escape && patterStr[i] == matchOne){
        if(c1 == matchOne){
          i++;
          continue;
        }
        else{
          return TSDB_PATTERN_NOMATCH;
        }
      }
      if (c == escape && patterStr[i] == matchAll){
        if(c1 == matchAll){
          i++;
          continue;
        }
        else{
          return TSDB_PATTERN_NOMATCH;
        }
      }
      if (c == c1 || towlower(c) == towlower(c1) || (c == matchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }
  
  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int32_t compareStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  assert(varDataLen(pRight) <= TSDB_MAX_FIELD_LEN);
  char *pattern = calloc(varDataLen(pRight) + 1, sizeof(char));
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));

  size_t sz = varDataLen(pLeft);
  char *buf = malloc(sz + 1);
  memcpy(buf, varDataVal(pLeft), sz);
  buf[sz] = 0;

  int32_t ret = patternMatch(pattern, buf, sz, &pInfo);
  free(buf);
  free(pattern);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
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

int32_t compareStrContainJson(const void* pLeft, const void* pRight) {
  if(pLeft) return 0;
  return 1;
}


int32_t taosArrayCompareString(const void* a, const void* b) {
  const char* x = *(const char**)a;
  const char* y = *(const char**)b;

  return compareLenPrefixedStr(x, y);
}

int32_t compareFindItemInSet(const void *pLeft, const void* pRight)  {
  return NULL != taosHashGet((SHashObj *)pRight, varDataVal(pLeft), varDataLen(pLeft)) ? 1 : 0;
}

int32_t compareWStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  size_t size = varDataLen(pLeft)/TSDB_NCHAR_SIZE;

  assert(varDataLen(pRight) <= TSDB_MAX_FIELD_LEN * TSDB_NCHAR_SIZE);

  wchar_t *pattern = calloc(varDataLen(pRight) + 1, sizeof(wchar_t));
  wchar_t *str = calloc(size + 1, sizeof(wchar_t));

  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));
  memcpy(str, varDataVal(pLeft), size * sizeof(wchar_t));

  int32_t ret = WCSPatternMatch((uint32_t *)pattern, (uint32_t *)str, size, &pInfo);

  free(pattern);
  free(str);

  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

__compar_fn_t getComparFunc(int32_t type, int32_t optr) {
  __compar_fn_t comparFn = NULL;

  if (optr == TSDB_RELATION_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
        return setCompareBytes1;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        return setCompareBytes2;
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_FLOAT:
        return setCompareBytes4;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
        return setCompareBytes8;
      default:
        assert(0);
    }
  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:   comparFn = compareInt8Val;   break;
    case TSDB_DATA_TYPE_SMALLINT:  comparFn = compareInt16Val;  break;
    case TSDB_DATA_TYPE_INT:       comparFn = compareInt32Val;  break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: comparFn = compareInt64Val;  break;
    case TSDB_DATA_TYPE_FLOAT:     comparFn = compareFloatVal;  break;
    case TSDB_DATA_TYPE_DOUBLE:    comparFn = compareDoubleVal; break;
    case TSDB_DATA_TYPE_BINARY: {
      if (optr == TSDB_RELATION_MATCH) {
        comparFn = compareStrRegexCompMatch;
      } else if (optr == TSDB_RELATION_NMATCH) {
        comparFn = compareStrRegexCompNMatch;
      } else if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparFn = compareStrPatternComp;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = compareFindItemInSet;
      } else { /* normal relational comparFn */
        comparFn = compareLenPrefixedStr;
      }

      break;
    }

    case TSDB_DATA_TYPE_NCHAR:{
      if (optr == TSDB_RELATION_MATCH) {
        comparFn = compareStrRegexCompMatch;
      } else if (optr == TSDB_RELATION_NMATCH) {
        comparFn = compareStrRegexCompNMatch;
      } else if (optr == TSDB_RELATION_LIKE) {
        comparFn = compareWStrPatternComp;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = compareFindItemInSet;
      } else {
        comparFn = compareLenPrefixedWStr;
      }
      break;
    }
    case TSDB_DATA_TYPE_JSON:{
      if (optr == TSDB_RELATION_MATCH) {
        comparFn = compareStrRegexCompMatch;
      } else if (optr == TSDB_RELATION_NMATCH) {
        comparFn = compareStrRegexCompNMatch;
      } else if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparFn = compareWStrPatternComp;
      } else if (optr == TSDB_RELATION_CONTAINS) {
        comparFn = compareStrContainJson;
      } else {
        comparFn = compareJsonVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT:  comparFn = compareUint8Val; break;
    case TSDB_DATA_TYPE_USMALLINT: comparFn = compareUint16Val;break;
    case TSDB_DATA_TYPE_UINT:      comparFn = compareUint32Val;break;
    case TSDB_DATA_TYPE_UBIGINT:   comparFn = compareUint64Val;break;

    default:
      comparFn = compareInt32Val;
      break;
  }

  return comparFn;
}

__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order) {
  __compar_fn_t comparFn = NULL;

  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
      comparFn = (order == TSDB_ORDER_ASC)? compareInt8Val:compareInt8ValDesc;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      comparFn = (order == TSDB_ORDER_ASC)? compareInt16Val:compareInt16ValDesc;
      break;
    case TSDB_DATA_TYPE_INT:
      comparFn = (order == TSDB_ORDER_ASC)? compareInt32Val:compareInt32ValDesc;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      comparFn = (order == TSDB_ORDER_ASC)? compareInt64Val:compareInt64ValDesc;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      comparFn = (order == TSDB_ORDER_ASC)? compareFloatVal:compareFloatValDesc;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = (order == TSDB_ORDER_ASC)? compareDoubleVal:compareDoubleValDesc;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      comparFn = (order == TSDB_ORDER_ASC)? compareUint8Val:compareUint8ValDesc;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      comparFn = (order == TSDB_ORDER_ASC)? compareUint16Val:compareUint16ValDesc;
      break;
    case TSDB_DATA_TYPE_UINT:
      comparFn = (order == TSDB_ORDER_ASC)? compareUint32Val:compareUint32ValDesc;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      comparFn = (order == TSDB_ORDER_ASC)? compareUint64Val:compareUint64ValDesc;
      break;
    case TSDB_DATA_TYPE_BINARY:
      comparFn = (order == TSDB_ORDER_ASC)? compareLenPrefixedStr:compareLenPrefixedStrDesc;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = (order == TSDB_ORDER_ASC)? compareLenPrefixedWStr:compareLenPrefixedWStrDesc;
      break;
    default:
      comparFn = (order == TSDB_ORDER_ASC)? compareInt32Val:compareInt32ValDesc;
      break;
  }

  return comparFn;
}

int32_t jsonCompareUnit(const char* f1, const char* f2, bool* canReturn){
  *canReturn = true;
  bool f1IsNull = (*f1 == TSDB_DATA_TYPE_JSON && isNull(f1 + CHAR_BYTES, TSDB_DATA_TYPE_JSON));
  bool f2IsNull = (*f2 == TSDB_DATA_TYPE_JSON && isNull(f2 + CHAR_BYTES, TSDB_DATA_TYPE_JSON));
  if(f1IsNull && f2IsNull){
    return 0;
  }else if(f1IsNull && !f2IsNull){
    return -1;
  }else if(!f1IsNull && f2IsNull){
    return 1;
  }else{
    bool f1IsJsonNull = (*f1 == TSDB_DATA_TYPE_BINARY && *(uint32_t*)varDataVal(f1 + CHAR_BYTES) == TSDB_DATA_JSON_null);
    bool f2IsJsonNull = (*f2 == TSDB_DATA_TYPE_BINARY && *(uint32_t*)varDataVal(f2 + CHAR_BYTES) == TSDB_DATA_JSON_null);
    if(f1IsJsonNull && f2IsJsonNull){
      return 0;
    }else if(f1IsJsonNull && !f2IsJsonNull){
      return -1;
    }else if(!f1IsJsonNull && f2IsJsonNull) {
      return 1;
    }
    if(*f1 != *f2 && !(IS_NUMERIC_TYPE(*f1) && IS_NUMERIC_TYPE(*f2))) {
      return *f1 > *f2 ? 1 : -1;
    }
    if(*f1 == TSDB_DATA_TYPE_BIGINT && *f2 == TSDB_DATA_TYPE_DOUBLE){
      DEFAULT_COMP(GET_INT64_VAL(f1 + CHAR_BYTES), GET_DOUBLE_VAL(f2 + CHAR_BYTES));
    }else if(*f1 == TSDB_DATA_TYPE_DOUBLE && *f2 == TSDB_DATA_TYPE_BIGINT){
      DEFAULT_COMP(GET_DOUBLE_VAL(f1 + CHAR_BYTES), GET_INT64_VAL(f2 + CHAR_BYTES));
    }
    *canReturn = false;
    return 0;   // meaningless
  }
}

int32_t doCompare(const char* f1, const char* f2, int32_t type, size_t size) {
  if (type == TSDB_DATA_TYPE_JSON){
    bool canReturn = true;
    int32_t result = jsonCompareUnit(f1, f2, &canReturn);
    if(canReturn) return result;
    type = *f1;
    f1 += CHAR_BYTES;
    f2 += CHAR_BYTES;
  }
  switch (type) {
    case TSDB_DATA_TYPE_INT:        DEFAULT_COMP(GET_INT32_VAL(f1), GET_INT32_VAL(f2));
    case TSDB_DATA_TYPE_DOUBLE:     DEFAULT_DOUBLE_COMP(GET_DOUBLE_VAL(f1), GET_DOUBLE_VAL(f2));
    case TSDB_DATA_TYPE_FLOAT:      DEFAULT_FLOAT_COMP(GET_FLOAT_VAL(f1), GET_FLOAT_VAL(f2));
    case TSDB_DATA_TYPE_BIGINT:     DEFAULT_COMP(GET_INT64_VAL(f1), GET_INT64_VAL(f2));
    case TSDB_DATA_TYPE_SMALLINT:   DEFAULT_COMP(GET_INT16_VAL(f1), GET_INT16_VAL(f2));
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:       DEFAULT_COMP(GET_INT8_VAL(f1), GET_INT8_VAL(f2));
    case TSDB_DATA_TYPE_UTINYINT:   DEFAULT_COMP(GET_UINT8_VAL(f1), GET_UINT8_VAL(f2));
    case TSDB_DATA_TYPE_USMALLINT:  DEFAULT_COMP(GET_UINT16_VAL(f1), GET_UINT16_VAL(f2));
    case TSDB_DATA_TYPE_UINT:       DEFAULT_COMP(GET_UINT32_VAL(f1), GET_UINT32_VAL(f2));
    case TSDB_DATA_TYPE_UBIGINT:    DEFAULT_COMP(GET_UINT64_VAL(f1), GET_UINT64_VAL(f2));
    case TSDB_DATA_TYPE_NCHAR:      return compareLenPrefixedWStr(f1, f2);
    default: // BINARY AND NULL AND SO ON
      return compareLenPrefixedStr(f1, f2);
  }
}
