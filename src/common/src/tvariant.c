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

#include "tvariant.h"
#include "hash.h"
#include "taos.h"
#include "taosdef.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tutil.h"
#include "ttype.h"

void tVariantCreate(tVariant *pVar, SStrToken *token) {
  int32_t ret = 0;
  int32_t type = token->type;

  memset(pVar, 0, sizeof(tVariant));

  switch (token->type) {
    case TSDB_DATA_TYPE_BOOL: {
      int32_t k = strncasecmp(token->z, "true", 4);
      if (k == 0) {
        pVar->i64 = TSDB_TRUE;
      } else {
        assert(strncasecmp(token->z, "false", 5) == 0);
        pVar->i64 = TSDB_FALSE;
      }

      break;
    }

    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_INT:{
      ret = tStrToInteger(token->z, token->type, token->n, &pVar->i64, true);
      if (ret != 0) {
        pVar->nType = -1;   // -1 means error type
        return;
      }

      break;
    }

    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT: {
      pVar->dKey = strtod(token->z, NULL);
      break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      pVar->pz = strndup(token->z, token->n);
      pVar->nLen = strdequote(pVar->pz);
      break;
    }
    
    default: {  // nType == 0 means the null value
      type = TSDB_DATA_TYPE_NULL;
    }
  }
  
  pVar->nType = type;
}

/**
 * create tVariant from binary string, not ascii data
 * @param pVar
 * @param pz
 * @param len
 * @param type
 */
void tVariantCreateFromBinary(tVariant *pVar, const char *pz, size_t len, uint32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      pVar->i64 = GET_INT8_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      pVar->u64 = GET_UINT8_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      pVar->i64 = GET_INT16_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      pVar->u64 = GET_UINT16_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      pVar->i64 = GET_INT32_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      pVar->u64 = GET_UINT32_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      pVar->i64 = GET_INT64_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      pVar->u64 = GET_UINT64_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      pVar->dKey = GET_DOUBLE_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      pVar->dKey = GET_FLOAT_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: { // here we get the nchar length from raw binary bits length
      size_t lenInwchar = len / TSDB_NCHAR_SIZE;

      pVar->wpz = calloc(1, (lenInwchar + 1) * TSDB_NCHAR_SIZE);
      memcpy(pVar->wpz, pz, lenInwchar * TSDB_NCHAR_SIZE);
      pVar->nLen = (int32_t)len;
      
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {  // todo refactor, extract a method
      pVar->pz = calloc(len + 1, sizeof(char));
      memcpy(pVar->pz, pz, len);
      pVar->nLen = (int32_t)len;
      break;
    }
    
    default:
      pVar->i64 = GET_INT32_VAL(pz);
  }
  
  pVar->nType = type;
}

void tVariantDestroy(tVariant *pVar) {
  if (pVar == NULL) return;
  
  if (pVar->nType == TSDB_DATA_TYPE_BINARY || pVar->nType == TSDB_DATA_TYPE_NCHAR) {
    tfree(pVar->pz);
    pVar->nLen = 0;
  }

  // NOTE: this is only for string array
  if (pVar->nType == TSDB_DATA_TYPE_ARRAY) {
    size_t num = taosArrayGetSize(pVar->arr);
    for(size_t i = 0; i < num; i++) {
      void* p = taosArrayGetP(pVar->arr, i);
      free(p);
    }
    taosArrayDestroy(pVar->arr);
    pVar->arr = NULL;
  }
}

bool tVariantIsValid(tVariant *pVar) {
  assert(pVar != NULL);
  return isValidDataType(pVar->nType);
}

void tVariantAssign(tVariant *pDst, const tVariant *pSrc) {
  if (pSrc == NULL || pDst == NULL) return;
  
  pDst->nType = pSrc->nType;
  if (pSrc->nType == TSDB_DATA_TYPE_BINARY || pSrc->nType == TSDB_DATA_TYPE_NCHAR) {
    int32_t len = pSrc->nLen + TSDB_NCHAR_SIZE;
    char* p = realloc(pDst->pz, len);
    assert(p);

    memset(p, 0, len);
    pDst->pz = p;

    memcpy(pDst->pz, pSrc->pz, pSrc->nLen);
    pDst->nLen = pSrc->nLen;
    return;

  }

  if (IS_NUMERIC_TYPE(pSrc->nType) || (pSrc->nType == TSDB_DATA_TYPE_BOOL)) {
    pDst->i64 = pSrc->i64;
  } else if (pSrc->nType == TSDB_DATA_TYPE_ARRAY) {  // this is only for string array
    size_t num = taosArrayGetSize(pSrc->arr);
    pDst->arr = taosArrayInit(num, sizeof(char*));
    for(size_t i = 0; i < num; i++) {
      char* p = (char*)taosArrayGetP(pSrc->arr, i);
      char* n = strdup(p);
      taosArrayPush(pDst->arr, &n);
    }
  }

  if (pDst->nType != TSDB_DATA_TYPE_ARRAY) {
    pDst->nLen = tDataTypes[pDst->nType].bytes;
  }
}

int32_t tVariantCompare(const tVariant* p1, const tVariant* p2) {
  if (p1->nType == TSDB_DATA_TYPE_NULL && p2->nType == TSDB_DATA_TYPE_NULL) {
    return 0;
  }

  if (p1->nType == TSDB_DATA_TYPE_NULL) {
    return -1;
  }

  if (p2->nType == TSDB_DATA_TYPE_NULL) {
    return 1;
  }

  if (p1->nType == TSDB_DATA_TYPE_BINARY || p1->nType == TSDB_DATA_TYPE_NCHAR) {
    if (p1->nLen == p2->nLen) {
      return memcmp(p1->pz, p2->pz, p1->nLen);
    } else {
      return p1->nLen > p2->nLen? 1:-1;
    }
  } else if (p1->nType == TSDB_DATA_TYPE_FLOAT || p1->nType == TSDB_DATA_TYPE_DOUBLE) {
    if (p1->dKey == p2->dKey) {
      return 0;
    } else {
      return p1->dKey > p2->dKey? 1:-1;
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(p1->nType)) {
    if (p1->u64 == p2->u64) {
      return 0;
    } else {
      return p1->u64 > p2->u64? 1:-1;
    }
  } else {
    if (p1->i64 == p2->i64) {
      return 0;
    } else {
      return p1->i64 > p2->i64? 1:-1;
    }
  }
}

int32_t tVariantToString(tVariant *pVar, char *dst) {
  if (pVar == NULL || dst == NULL) return 0;
  
  switch (pVar->nType) {
    case TSDB_DATA_TYPE_BINARY: {
      int32_t len = sprintf(dst, "\'%s\'", pVar->pz);
      assert(len <= pVar->nLen + sizeof("\'") * 2);  // two more chars
      return len;
    }
    
    case TSDB_DATA_TYPE_NCHAR: {
      dst[0] = '\'';
      taosUcs4ToMbs(pVar->wpz, (twcslen(pVar->wpz) + 1) * TSDB_NCHAR_SIZE, dst + 1);
      int32_t len = (int32_t)strlen(dst);
      dst[len] = '\'';
      dst[len + 1] = 0;
      return len + 1;
    }
    
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
      return sprintf(dst, "%d", (int32_t)pVar->i64);
    
    case TSDB_DATA_TYPE_BIGINT:
      return sprintf(dst, "%" PRId64, pVar->i64);
    case TSDB_DATA_TYPE_UBIGINT:
      return sprintf(dst, "%" PRIu64, pVar->u64);
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return sprintf(dst, "%.9lf", pVar->dKey);
    
    default:
      return 0;
  }
}

static FORCE_INLINE int32_t convertToBoolImpl(char *pStr, int32_t len) {
  if ((strncasecmp(pStr, "true", len) == 0) && (len == 4)) {
    return TSDB_TRUE;
  } else if ((strncasecmp(pStr, "false", len) == 0) && (len == 5)) {
    return TSDB_FALSE;
  } else if (strcasecmp(pStr, TSDB_DATA_NULL_STR_L) == 0) {
    return TSDB_DATA_BOOL_NULL;
  } else {
    return -1;
  }
}

static FORCE_INLINE int32_t wcsconvertToBoolImpl(wchar_t *pstr, int32_t len) {
  if ((wcsncasecmp(pstr, L"true", len) == 0) && (len == 4)) {
    return TSDB_TRUE;
  } else if (wcsncasecmp(pstr, L"false", len) == 0 && (len == 5)) {
    return TSDB_FALSE;
  } else if (memcmp(pstr, L"null", wcslen(L"null")) == 0) {
    return TSDB_DATA_BOOL_NULL;
  } else {
    return -1;
  }
}

static int32_t toBinary(tVariant *pVariant, char **pDest, int32_t *pDestSize) {
  const int32_t INITIAL_ALLOC_SIZE = 40;
  char *        pBuf = NULL;

  // it is a in-place convert type for tVariant, local buffer is needed
  if (*pDest == pVariant->pz) {
    pBuf = calloc(1, INITIAL_ALLOC_SIZE);
  }
  
  if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
    size_t newSize = pVariant->nLen * TSDB_NCHAR_SIZE;
    if (pBuf != NULL) {
      if (newSize >= INITIAL_ALLOC_SIZE) {
        pBuf = realloc(pBuf, newSize + 1);
      }
      
      taosUcs4ToMbs(pVariant->wpz, (int32_t)newSize, pBuf);
      free(pVariant->wpz);
      pBuf[newSize] = 0;
    } else {
      taosUcs4ToMbs(pVariant->wpz, (int32_t)newSize, *pDest);
    }
    
  } else {
    if (IS_SIGNED_NUMERIC_TYPE(pVariant->nType)) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%" PRId64, pVariant->i64);
    } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%lf", pVariant->dKey);
    } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%s", (pVariant->i64 == TSDB_TRUE) ? "TRUE" : "FALSE");
    } else if (pVariant->nType == 0) {  // null data
      setNull(pBuf == NULL ? *pDest : pBuf, TSDB_DATA_TYPE_BINARY, 0);
    }
  }
  
  if (pBuf != NULL) {
    *pDest = pBuf;
  }
  
  *pDestSize = (int32_t)strlen(*pDest);
  return 0;
}

static int32_t toNchar(tVariant *pVariant, char **pDest, int32_t *pDestSize) {
  char tmpBuf[40] = {0};
  
  char *  pDst = tmpBuf;
  int32_t nLen = 0;

  // convert the number to string, than convert it to wchar string.
  if (IS_SIGNED_NUMERIC_TYPE(pVariant->nType)) {
    nLen = sprintf(pDst, "%" PRId64, pVariant->i64);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pVariant->nType)) {
    nLen = sprintf(pDst, "%"PRIu64, pVariant->u64);
  } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
    nLen = sprintf(pDst, "%lf", pVariant->dKey);
  } else if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
    pDst = pVariant->pz;
    nLen = pVariant->nLen;
  } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
    nLen = sprintf(pDst, "%s", (pVariant->i64 == TSDB_TRUE) ? "TRUE" : "FALSE");
  }
  
  if (*pDest == pVariant->pz) {
    wchar_t *pWStr = calloc(1, (nLen + 1) * TSDB_NCHAR_SIZE);
    bool ret = taosMbsToUcs4(pDst, nLen, (char *)pWStr, (nLen + 1) * TSDB_NCHAR_SIZE, NULL);
    if (!ret) {
      return -1;
    }

    // free the binary buffer in the first place
    if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
      free(pVariant->wpz);
    }
    
    pVariant->wpz = pWStr;
    *pDestSize = twcslen(pVariant->wpz);
    
    // shrink the allocate memory, no need to check here.
    char* tmp = realloc(pVariant->wpz, (*pDestSize + 1)*TSDB_NCHAR_SIZE);
    assert(tmp != NULL);
    
    pVariant->wpz = (wchar_t *)tmp;
  } else {
    int32_t output = 0;

    bool ret = taosMbsToUcs4(pDst, nLen, *pDest, (nLen + 1) * TSDB_NCHAR_SIZE, &output);
    if (!ret) {
      return -1;
    }

    if (pDestSize != NULL) {
      *pDestSize = output;
    }
  }
  
  return 0;
}

static FORCE_INLINE int32_t convertToDouble(char *pStr, int32_t len, double *value) {
  SStrToken stoken = {.z = pStr, .n = len};
  if (TK_ILLEGAL == tGetNumericStringType(&stoken)) {
    return -1;
  }
  
  *value = strtod(pStr, NULL);
  return 0;
}

static FORCE_INLINE int32_t convertToInteger(tVariant *pVariant, int64_t *result, int32_t type, bool issigned, bool releaseVariantPtr) {
  if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
    setNull((char *)result, type, tDataTypes[type].bytes);
    return 0;
  }

  errno = 0;
  if (IS_SIGNED_NUMERIC_TYPE(pVariant->nType) || (pVariant->nType == TSDB_DATA_TYPE_BOOL)) {
    *result = pVariant->i64;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pVariant->nType)) {
    *result = pVariant->u64;
  } else if (IS_FLOAT_TYPE(pVariant->nType)) {
    *result = (int64_t) pVariant->dKey;
  } else if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
    SStrToken token = {.z = pVariant->pz, .n = pVariant->nLen};
    /*int32_t n = */tSQLGetToken(pVariant->pz, &token.type);

    if (token.type == TK_NULL) {
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }

      setNull((char *)result, type, tDataTypes[type].bytes);
      return 0;
    }

    // decide if it is a valid number
    token.type = tGetNumericStringType(&token);
    if (token.type == TK_ILLEGAL) {
      return -1;
    }

    int64_t res = 0;
    int32_t t = tStrToInteger(token.z, token.type, token.n, &res, issigned);
    if (t != 0) {
      return -1;
    }

    if (releaseVariantPtr) {
      free(pVariant->pz);
      pVariant->nLen = 0;
    }

    *result = res;
  } else if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
    errno = 0;
    wchar_t *endPtr = NULL;
    
    SStrToken token = {0};
    token.n = tSQLGetToken(pVariant->pz, &token.type);
    
    if (token.type == TK_MINUS || token.type == TK_PLUS) {
      token.n = tSQLGetToken(pVariant->pz + token.n, &token.type);
    }
    
    if (token.type == TK_FLOAT) {
      double v = wcstod(pVariant->wpz, &endPtr);
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }
      
      if ((errno == ERANGE && v == -1) || (isinf(v) || isnan(v))) {
        return -1;
      }
      
      *result = (int64_t)v;
    } else if (token.type == TK_NULL) {
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }
      setNull((char *)result, type, tDataTypes[type].bytes);
      return 0;
    } else {
      int64_t val = wcstoll(pVariant->wpz, &endPtr, 10);
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }
      
      if (errno == ERANGE) {
        return -1;  // data overflow
      }
      
      *result = val;
    }
  }

  bool code = false;
  switch(type) {
    case TSDB_DATA_TYPE_TINYINT:
      code = IS_VALID_TINYINT(*result); break;
    case TSDB_DATA_TYPE_SMALLINT:
      code = IS_VALID_SMALLINT(*result); break;
    case TSDB_DATA_TYPE_INT:
      code = IS_VALID_INT(*result); break;
    case TSDB_DATA_TYPE_BIGINT:
      code = IS_VALID_BIGINT(*result); break;
    case TSDB_DATA_TYPE_UTINYINT:
      code = IS_VALID_UTINYINT(*result); break;
    case TSDB_DATA_TYPE_USMALLINT:
      code = IS_VALID_USMALLINT(*result); break;
    case TSDB_DATA_TYPE_UINT:
      code = IS_VALID_UINT(*result); break;
    case TSDB_DATA_TYPE_UBIGINT:
      code = IS_VALID_UBIGINT(*result); break;
  }

  return code? 0:-1;
}

static int32_t convertToBool(tVariant *pVariant, int64_t *pDest) {
  if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
    *pDest = pVariant->i64;  // in order to be compatible to null of bool
  } else if (IS_NUMERIC_TYPE(pVariant->nType)) {
    *pDest = ((pVariant->i64 != 0) ? TSDB_TRUE : TSDB_FALSE);
  } else if (pVariant->nType == TSDB_DATA_TYPE_FLOAT || pVariant->nType == TSDB_DATA_TYPE_DOUBLE) {
    *pDest = ((pVariant->dKey != 0) ? TSDB_TRUE : TSDB_FALSE);
  } else if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
    int32_t ret = 0;
    if ((ret = convertToBoolImpl(pVariant->pz, pVariant->nLen)) < 0) {
      return ret;
    }
    
    *pDest = ret;
  } else if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
    int32_t ret = 0;
    if ((ret = wcsconvertToBoolImpl(pVariant->wpz, pVariant->nLen)) < 0) {
      return ret;
    }
    *pDest = ret;
  } else if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
    *pDest = TSDB_DATA_BOOL_NULL;
  }
  
  assert(*pDest == TSDB_TRUE || *pDest == TSDB_FALSE || *pDest == TSDB_DATA_BOOL_NULL);
  return 0;
}

/*
 * transfer data from variant serve as the implicit data conversion: from input sql string pVariant->nType
 * to column type defined in schema
 */
int32_t tVariantDump(tVariant *pVariant, char *payload, int16_t type, bool includeLengthPrefix) {
  if (pVariant == NULL || (pVariant->nType != 0 && !isValidDataType(pVariant->nType))) {
    return -1;
  }

  errno = 0;  // reset global error code
  int64_t result;

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      if (convertToBool(pVariant, &result) < 0) {
        return -1;
      }

      *(int8_t *)payload = (int8_t)result;
      break;
    }
    
    case TSDB_DATA_TYPE_TINYINT: {
      if (convertToInteger(pVariant, &result, type, true, false) < 0) {
        return -1;
      }
      *((int8_t *)payload) = (int8_t) result;
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      if (convertToInteger(pVariant, &result, type, false, false) < 0) {
        return -1;
      }
      *((uint8_t *)payload) = (uint8_t) result;
      break;
    }
    
    case TSDB_DATA_TYPE_SMALLINT: {
      if (convertToInteger(pVariant, &result, type, true, false) < 0) {
        return -1;
      }
      *((int16_t *)payload) = (int16_t)result;
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      if (convertToInteger(pVariant, &result, type, false, false) < 0) {
        return -1;
      }
      *((uint16_t *)payload) = (uint16_t)result;
      break;
    }
    
    case TSDB_DATA_TYPE_INT: {
      if (convertToInteger(pVariant, &result, type, true, false) < 0) {
        return -1;
      }
      *((int32_t *)payload) = (int32_t)result;
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      if (convertToInteger(pVariant, &result, type, false, false) < 0) {
        return -1;
      }
      *((uint32_t *)payload) = (uint32_t)result;
      break;
    }
    
    case TSDB_DATA_TYPE_BIGINT: {
      if (convertToInteger(pVariant, &result, type, true, false) < 0) {
        return -1;
      }
      *((int64_t *)payload) = (int64_t)result;
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      if (convertToInteger(pVariant, &result, type, false, false) < 0) {
        return -1;
      }
      *((uint64_t *)payload) = (uint64_t)result;
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
        if (strncasecmp(TSDB_DATA_NULL_STR_L, pVariant->pz, pVariant->nLen) == 0 &&
            strlen(TSDB_DATA_NULL_STR_L) == pVariant->nLen) {
          *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
          return 0;
        } else {
          double  value = -1;
          int32_t ret = convertToDouble(pVariant->pz, pVariant->nLen, &value);
          if ((errno == ERANGE && (float)value == -1) || (ret != 0)) {
            return -1;
          }

          SET_FLOAT_VAL(payload, value);
        }
      } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL || IS_SIGNED_NUMERIC_TYPE(pVariant->nType) || IS_UNSIGNED_NUMERIC_TYPE(pVariant->nType)) {
        SET_FLOAT_VAL(payload, pVariant->i64);
      } else if (IS_FLOAT_TYPE(pVariant->nType)) {
        SET_FLOAT_VAL(payload, pVariant->dKey);
      } else if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
        *((uint32_t *)payload) = TSDB_DATA_FLOAT_NULL;
        return 0;
      }

      float fv = GET_FLOAT_VAL(payload);
      if (isinf(fv) || isnan(fv) || fv > FLT_MAX || fv < -FLT_MAX) {
        return -1;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
        if (strncasecmp(TSDB_DATA_NULL_STR_L, pVariant->pz, pVariant->nLen) == 0 &&
            strlen(TSDB_DATA_NULL_STR_L) == pVariant->nLen) {
          *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
          return 0;
        } else {
          double  value = 0;
          int32_t ret;
          ret = convertToDouble(pVariant->pz, pVariant->nLen, &value);
          if ((errno == ERANGE && value == -1) || (ret != 0)) {
            return -1;
          }

          SET_DOUBLE_VAL(payload, value);
        }
      } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL || IS_SIGNED_NUMERIC_TYPE(pVariant->nType) || IS_UNSIGNED_NUMERIC_TYPE(pVariant->nType)) {
        SET_DOUBLE_VAL(payload, pVariant->i64);
      } else if (IS_FLOAT_TYPE(pVariant->nType)) {
        SET_DOUBLE_VAL(payload, pVariant->dKey);
      } else if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
        return 0;
      }

      double dv = GET_DOUBLE_VAL(payload);
      if (errno == ERANGE || isinf(dv) || isnan(dv)) {
        return -1;
      }

      break;
    }
    
    case TSDB_DATA_TYPE_BINARY: {
      if (!includeLengthPrefix) {
        if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
          *(uint8_t*) payload = TSDB_DATA_BINARY_NULL;
        } else {
          if (pVariant->nType != TSDB_DATA_TYPE_BINARY) {
            toBinary(pVariant, &payload, &pVariant->nLen);
          } else {
            strncpy(payload, pVariant->pz, pVariant->nLen);
          }
        }
      } else {
        if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
          setVardataNull(payload, TSDB_DATA_TYPE_BINARY);
        } else {
          char *p = varDataVal(payload);

          if (pVariant->nType != TSDB_DATA_TYPE_BINARY) {
            toBinary(pVariant, &p, &pVariant->nLen);
          } else {
            strncpy(p, pVariant->pz, pVariant->nLen);
          }

          varDataSetLen(payload, pVariant->nLen);
          assert(p == varDataVal(payload));
        }
      }
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else {
        *((int64_t *)payload) = pVariant->i64;
      }
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      int32_t newlen = 0;
      if (!includeLengthPrefix) {
        if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
          *(uint32_t *)payload = TSDB_DATA_NCHAR_NULL;
        } else {
          if (pVariant->nType != TSDB_DATA_TYPE_NCHAR) {
            if (toNchar(pVariant, &payload, &newlen) != 0) {
              return -1;
            }
          } else {
            wcsncpy((wchar_t *)payload, pVariant->wpz, pVariant->nLen);
          }
        }
      } else {
        if (pVariant->nType == TSDB_DATA_TYPE_NULL) {
          setVardataNull(payload, TSDB_DATA_TYPE_NCHAR);
        } else {
          char *p = varDataVal(payload);

          if (pVariant->nType != TSDB_DATA_TYPE_NCHAR) {
            if (toNchar(pVariant, &p, &newlen) != 0) {
              return -1;
            }
          } else {
            wcsncpy((wchar_t *)p, pVariant->wpz, pVariant->nLen);
            newlen = pVariant->nLen;
          }

          varDataSetLen(payload, newlen);  // the length may be changed after toNchar function called
          assert(p == varDataVal(payload));
        }
      }
      
      break;
    }
  }
  
  return 0;
}

/*
 * In variant, bool/smallint/tinyint/int/bigint share the same attribution of
 * structure, also ignore the convert the type required
 *
 * It is actually the bigint/binary/bool/nchar type transfer
 */
int32_t tVariantTypeSetType(tVariant *pVariant, char type) {
  if (pVariant == NULL || pVariant->nType == 0) {  // value is not set
    return 0;
  }
  
  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if (convertToBool(pVariant, &pVariant->i64) < 0) {
        return -1;
      }
      
      pVariant->nType = type;
      break;
    }
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      convertToInteger(pVariant, &(pVariant->i64), type, true, true);
      pVariant->nType = TSDB_DATA_TYPE_BIGINT;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
        errno = 0;
        double v = strtod(pVariant->pz, NULL);
        if ((errno == ERANGE && v == -1) || (isinf(v) || isnan(v))) {
          free(pVariant->pz);
          return -1;
        }
        
        free(pVariant->pz);
        pVariant->dKey = v;
      } else if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
        errno = 0;
        double v = wcstod(pVariant->wpz, NULL);
        if ((errno == ERANGE && v == -1) || (isinf(v) || isnan(v))) {
          free(pVariant->pz);
          return -1;
        }
        
        free(pVariant->pz);
        pVariant->dKey = v;
      } else if (pVariant->nType >= TSDB_DATA_TYPE_BOOL && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
        pVariant->dKey = (double)(pVariant->i64);
      }
      
      pVariant->nType = TSDB_DATA_TYPE_DOUBLE;
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      if (pVariant->nType != TSDB_DATA_TYPE_BINARY) {
        toBinary(pVariant, &pVariant->pz, &pVariant->nLen);
      }
      pVariant->nType = type;
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      if (pVariant->nType != TSDB_DATA_TYPE_NCHAR) {
        if (toNchar(pVariant, &pVariant->pz, &pVariant->nLen) != 0) {
          return -1;
        }
      }
      pVariant->nType = type;
      break;
    }
  }
  
  return 0;
}
