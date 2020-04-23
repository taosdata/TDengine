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

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double ret = GET_DOUBLE_VAL(pLeft) - GET_DOUBLE_VAL(pRight);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareStrVal(const void *pLeft, const void *pRight) {
  return (int32_t)strcmp(pLeft, pRight);
}

int32_t compareWStrVal(const void *pLeft, const void *pRight) {
  //  SSkipListKey *pL = (SSkipListKey *)pLeft;
  //  SSkipListKey *pR = (SSkipListKey *)pRight;
  //
  //  if (pL->nLen == 0 && pR->nLen == 0) {
  //    return 0;
  //  }
  //
  //  // handle only one-side bound compare situation, there is only lower bound or only upper bound
  //  if (pL->nLen == -1) {
  //    return 1;  // no lower bound, lower bound is minimum, always return -1;
  //  } else if (pR->nLen == -1) {
  //    return -1;  // no upper bound, upper bound is maximum situation, always return 1;
  //  }
  //
  //  int32_t ret = wcscmp(((SSkipListKey *)pLeft)->wpz, ((SSkipListKey *)pRight)->wpz);
  //
  //  if (ret == 0) {
  //    return 0;
  //  } else {
  //    return ret > 0 ? 1 : -1;
  //  }
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
        
        if (str[0] == 0 || (n >= size - 1)) {
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
        size_t n = wcsspn(str, accept);
        
        str += n;
        if (str[0] == 0 || (n >= size - 1)) {
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

static UNUSED_FUNC int32_t compareStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  const char* pattern = pRight;
  const char* str = pLeft;
  
  int32_t ret = patternMatch(pattern, str, strlen(str), &pInfo);
  
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

static int32_t compareStrInList(const void* pLeft, const void* pRight) {
  const SArray* arr = (const SArray*)pRight;
  return taosArraySearchString(arr, &pLeft) == NULL ? 0 : 1;
}

static UNUSED_FUNC int32_t compareWStrPatternComp(const void* pLeft, const void* pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};
  
  const wchar_t* pattern = pRight;
  const wchar_t* str = pLeft;
  
  int32_t ret = WCSPatternMatch(pattern, str, wcslen(str), &pInfo);
  
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

// todo promote the type definition before the comparsion
__compar_fn_t getComparFunc(int32_t type, int32_t filterDataType, int32_t optr) {
  __compar_fn_t comparFn = NULL;
  
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
//      assert(type == filterDataType);
      if (filterDataType == TSDB_DATA_TYPE_BIGINT || filterDataType == TSDB_DATA_TYPE_TIMESTAMP) {
        comparFn = compareInt64Val;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareIntDoubleVal;
      }
      
      break;
    }

    case TSDB_DATA_TYPE_BOOL: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareInt32Val;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareIntDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareDoubleIntVal;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        assert(filterDataType == TSDB_DATA_TYPE_BINARY);
        comparFn = compareStrPatternComp;

      } else if (optr == TSDB_RELATION_IN) {
        assert(filterDataType == TSDB_DATA_TYPE_ARRAY);
        comparFn = compareStrInList;

      } else { /* normal relational comparFn */
        assert(filterDataType == TSDB_DATA_TYPE_BINARY);
        comparFn = compareStrVal;
      }
    
      break;
    }
  
    case TSDB_DATA_TYPE_NCHAR: {
      assert(filterDataType == TSDB_DATA_TYPE_NCHAR);
    
      if (optr == TSDB_RELATION_LIKE) {
        comparFn = compareWStrPatternComp;
      } else {
        comparFn = compareWStrVal;
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
    case TSDB_DATA_TYPE_BOOL:
      comparFn = compareInt32Val;
      break;
    
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = compareDoubleVal;
      break;
    
    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareStrVal;
      break;
    
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareWStrVal;
      break;
    
    default:
      comparFn = compareInt32Val;
      break;
  }
  
  return comparFn;
}
