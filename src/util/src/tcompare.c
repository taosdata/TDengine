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

#include "os.h"
#include "ttype.h"
#include "tcompare.h"
#include "hash.h"

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
    int32_t ret = wcsncmp(varDataVal(pLeft), varDataVal(pRight), len1/TSDB_NCHAR_SIZE);
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
  
  while ((c = patterStr[i++]) != 0) {
    if (c == pInfo->matchAll) { /* Match "*" */
      
      while ((c = patterStr[i++]) == pInfo->matchAll || c == pInfo->matchOne) {
        if (c == pInfo->matchOne && (j > size || str[j++] == 0)) {
          // empty string, return not match
          return TSDB_PATTERN_NOWILDCARDMATCH;
        }
      }
      
      if (c == 0) {
        return TSDB_PATTERN_MATCH; /* "*" at the end of the pattern matches */
      }
      
      char next[3] = {toupper(c), tolower(c), 0};
      while (1) {
        size_t n = strcspn(str, next);
        str += n;
        
        if (str[0] == 0 || (n >= size)) {
          break;
        }
        
        int32_t ret = patternMatch(&patterStr[i], ++str, size - n - 1, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }
      return TSDB_PATTERN_NOWILDCARDMATCH;
    }
    
    c1 = str[j++];
    
    if (j <= size) {
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
        if (c == matchOne && (j > size || str[j++] == 0)) {
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

  assert(varDataLen(pRight) <= TSDB_MAX_FIELD_LEN * TSDB_NCHAR_SIZE);
  wchar_t *pattern = calloc(varDataLen(pRight) + 1, sizeof(wchar_t));

  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));

  int32_t ret = WCSPatternMatch(pattern, varDataVal(pLeft), varDataLen(pLeft)/TSDB_NCHAR_SIZE, &pInfo);
  free(pattern);
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
      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparFn = compareStrPatternComp;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = compareFindItemInSet;
      } else { /* normal relational comparFn */
        comparFn = compareLenPrefixedStr;
      }
    
      break;
    }
  
    case TSDB_DATA_TYPE_NCHAR: {
      if (optr == TSDB_RELATION_LIKE) {
        comparFn = compareWStrPatternComp;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = compareFindItemInSet;
      } else {
        comparFn = compareLenPrefixedWStr;
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

int32_t doCompare(const char* f1, const char* f2, int32_t type, size_t size) {
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
    case TSDB_DATA_TYPE_NCHAR: {
      tstr* t1 = (tstr*) f1;
      tstr* t2 = (tstr*) f2;

      if (t1->len != t2->len) {
        return t1->len > t2->len? 1:-1;
      }

      int32_t ret = wcsncmp((wchar_t*) t1->data, (wchar_t*) t2->data, t2->len/TSDB_NCHAR_SIZE);
      if (ret == 0) {
        return ret;
      }
      return (ret < 0) ? -1 : 1;
    }
    default: {  // todo refactor
      tstr* t1 = (tstr*) f1;
      tstr* t2 = (tstr*) f2;
      
      if (t1->len != t2->len) {
        return t1->len > t2->len? 1:-1;
      } else {
        int32_t ret = strncmp(t1->data, t2->data, t1->len);
        if (ret == 0) {
          return 0;
        } else {
          return ret < 0? -1:1;
        }
      }
    }
  }
}
