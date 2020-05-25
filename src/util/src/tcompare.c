#include "taosdef.h"
#include "tcompare.h"
#include <tarray.h>
#include "tutil.h"

int32_t compareInt32Val(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft), right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Val(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft), right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Val(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft), right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Val(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft), right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareIntDoubleVal(const void *pLeft, const void *pRight) {
  int64_t lhs = GET_INT64_VAL(pLeft);
  double  rhs = GET_DOUBLE_VAL(pRight);
  if (fabs(lhs - rhs) < FLT_EPSILON) {
    return 0;
  } else {
    return (lhs > rhs) ? 1 : -1;
  }
}

int32_t compareDoubleIntVal(const void *pLeft, const void *pRight) {
  double  lhs = GET_DOUBLE_VAL(pLeft);
  int64_t rhs = GET_INT64_VAL(pRight);
  if (fabs(lhs - rhs) < FLT_EPSILON) {
    return 0;
  } else {
    return (lhs > rhs) ? 1 : -1;
  }
}

int32_t compareFloatVal(const void *pLeft, const void *pRight) {
  float ret = GET_FLOAT_VAL(pLeft) - GET_FLOAT_VAL(pRight);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0? 1 : -1;
  }
}

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double ret = GET_DOUBLE_VAL(pLeft) - GET_DOUBLE_VAL(pRight);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0? 1 : -1;
  }
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

int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);
  
  if (len1 != len2) {
    return len1 > len2? 1:-1;
  } else {
    int32_t ret = wcsncmp(varDataVal(pLeft), varDataVal(pRight), len1);
    if (ret == 0) {
      return 0;
    } else {
      return ret > 0 ? 1 : -1;
    }
  }
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
        
        str++;
        
        int32_t ret = WCSPatternMatch(&patterStr[i], str, wcslen(str), pInfo);
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

static int32_t compareStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  char pattern[128] = {0};
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight));
  assert(varDataLen(pRight) < 128);
  
  int32_t ret = patternMatch(pattern, varDataVal(pLeft), varDataLen(pLeft), &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t taosArrayCompareString(const void* a, const void* b) {
  const char* x = *(const char**)a;
  const char* y = *(const char**)b;
  
  return compareLenPrefixedStr(x, y);
}

static int32_t compareFindStrInArray(const void* pLeft, const void* pRight) {
  const SArray* arr = (const SArray*) pRight;
  return taosArraySearchString(arr, pLeft, taosArrayCompareString) == NULL ? 0 : 1;
}

static int32_t compareWStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  wchar_t pattern[128] = {0};
  memcpy(pattern, varDataVal(pRight), varDataLen(pRight)/TSDB_NCHAR_SIZE);
  assert(varDataLen(pRight) < 128);
  
  int32_t ret = WCSPatternMatch(pattern, varDataVal(pLeft), varDataLen(pLeft)/TSDB_NCHAR_SIZE, &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

__compar_fn_t getComparFunc(int32_t type, int32_t optr) {
  __compar_fn_t comparFn = NULL;
  
  switch (type) {
    case TSDB_DATA_TYPE_SMALLINT: {
      comparFn = compareInt16Val;  break;
    }
    
    case TSDB_DATA_TYPE_INT: {
      comparFn = compareInt32Val;  break;
    }
    
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      comparFn = compareInt64Val;  break;
    }

    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:{
      comparFn = compareInt8Val;   break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      comparFn = compareFloatVal;  break;
    }
    
    case TSDB_DATA_TYPE_DOUBLE: {
      comparFn = compareDoubleVal; break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparFn = compareStrPatternComp;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = compareFindStrInArray;
      } else { /* normal relational comparFn */
        comparFn = compareLenPrefixedStr;
      }
    
      break;
    }
  
    case TSDB_DATA_TYPE_NCHAR: {
      if (optr == TSDB_RELATION_LIKE) {
        comparFn = compareWStrPatternComp;
      } else {
        comparFn = compareLenPrefixedWStr;
      }
    
      break;
    }
    
    default:
      comparFn = compareInt32Val;
      break;
  }
  
  return comparFn;
}

__compar_fn_t getKeyComparFunc(int32_t keyType) {
  __compar_fn_t comparFn = NULL;
  
  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:
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
    
    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareLenPrefixedStr;
      break;
    
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareLenPrefixedWStr;
      break;
    
    default:
      comparFn = compareInt32Val;
      break;
  }
  
  return comparFn;
}

int32_t doCompare(const char* f1, const char* f2, int32_t type, size_t size) {
  switch (type) {
    case TSDB_DATA_TYPE_INT:        DEFAULT_COMP(GET_INT32_VAL(f1), GET_INT32_VAL(f2));
    case TSDB_DATA_TYPE_DOUBLE:     DEFAULT_COMP(GET_DOUBLE_VAL(f1), GET_DOUBLE_VAL(f2));
    case TSDB_DATA_TYPE_FLOAT:      DEFAULT_COMP(GET_FLOAT_VAL(f1), GET_FLOAT_VAL(f2));
    case TSDB_DATA_TYPE_BIGINT:     DEFAULT_COMP(GET_INT64_VAL(f1), GET_INT64_VAL(f2));
    case TSDB_DATA_TYPE_SMALLINT:   DEFAULT_COMP(GET_INT16_VAL(f1), GET_INT16_VAL(f2));
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_BOOL:       DEFAULT_COMP(GET_INT8_VAL(f1), GET_INT8_VAL(f2));
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t ret = wcsncmp((wchar_t*) f1, (wchar_t*) f2, size/TSDB_NCHAR_SIZE);
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
