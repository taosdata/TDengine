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

#include <assert.h>
#include <errno.h>
#include <float.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "taos.h"
#include "tsdb.h"
#include "tsqldef.h"
#include "ttypes.h"
#include "tutil.h"

tDataDescriptor tDataTypeDesc[11] = {
  {TSDB_DATA_TYPE_NULL,      6, 1,            "NOTYPE"},
  {TSDB_DATA_TYPE_BOOL,      4, CHAR_BYTES,   "BOOL"},
  {TSDB_DATA_TYPE_TINYINT,   7, CHAR_BYTES,   "TINYINT"},
  {TSDB_DATA_TYPE_SMALLINT,  8, SHORT_BYTES,  "SMALLINT"},
  {TSDB_DATA_TYPE_INT,       3, INT_BYTES,    "INT"},
  {TSDB_DATA_TYPE_BIGINT,    6, LONG_BYTES,   "BIGINT"},
  {TSDB_DATA_TYPE_FLOAT,     5, FLOAT_BYTES,  "FLOAT"},
  {TSDB_DATA_TYPE_DOUBLE,    6, DOUBLE_BYTES, "DOUBLE"},
  {TSDB_DATA_TYPE_BINARY,    6, 0,            "BINARY"},
  {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES,   "TIMESTAMP"},
  {TSDB_DATA_TYPE_NCHAR,     5, 8,            "NCHAR"},
};

char tTokenTypeSwitcher[] = {
    TSDB_DATA_TYPE_NULL,    // no type
    TSDB_DATA_TYPE_BINARY,  // TK_ID
    TSDB_DATA_TYPE_BOOL,    // TK_BOOL
    TSDB_DATA_TYPE_BIGINT,  // TK_TINYINT
    TSDB_DATA_TYPE_BIGINT,  // TK_SMALLINT
    TSDB_DATA_TYPE_BIGINT,  // TK_INTEGER
    TSDB_DATA_TYPE_BIGINT,  // TK_BIGINT
    TSDB_DATA_TYPE_DOUBLE,  // TK_FLOAT
    TSDB_DATA_TYPE_DOUBLE,  // TK_DOUBLE
    TSDB_DATA_TYPE_BINARY,  // TK_STRING
    TSDB_DATA_TYPE_BIGINT,  // TK_TIMESTAMP
    TSDB_DATA_TYPE_BINARY,  // TK_BINARY
    TSDB_DATA_TYPE_NCHAR,   // TK_NCHAR
};

bool isValidDataType(int32_t type, int32_t length) {
  if (type < TSDB_DATA_TYPE_BOOL || type > TSDB_DATA_TYPE_NCHAR) {
    return false;
  }

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    return length >= 0 && length <= TSDB_MAX_BINARY_LEN;
  }

  return true;
}

// todo support scientific expression number and oct number
void tVariantCreate(tVariant *pVar, SSQLToken *token) { tVariantCreateN(pVar, token->z, token->n, token->type); }

void tVariantCreateN(tVariant *pVar, char *pz, uint32_t len, uint32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {
      int32_t k = strncasecmp(pz, "true", 4);
      if (k == 0) {
        pVar->i64Key = TSDB_TRUE;
      } else {
        assert(strncasecmp(pz, "false", 5) == 0);
        pVar->i64Key = TSDB_FALSE;
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_INT:
      pVar->i64Key = strtoll(pz, NULL, 10);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_FLOAT:
      pVar->dKey = strtod(pz, NULL);
      break;
    case TSDB_DATA_TYPE_BINARY: {
      pVar->pz = malloc(len + 1);
      strncpy(pVar->pz, pz, len);
      pVar->nLen = len;
      pVar->pz[len] = 0;

      pVar->nLen = strdequote(pVar->pz);
      break;
    }
    default:
      assert(false);
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
void tVariantCreateB(tVariant *pVar, char *pz, uint32_t len, uint32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      pVar->i64Key = *(int8_t *)pz;
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      pVar->i64Key = *(int16_t *)pz;
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      pVar->i64Key = *(int32_t *)pz;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      pVar->i64Key = *(int64_t *)pz;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      pVar->dKey = *(double *)pz;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      pVar->dKey = *(float *)pz;
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      /* here we get the nchar length from raw binary bits length */
      int32_t wlen = len / TSDB_NCHAR_SIZE;
      pVar->wpz = malloc((len + 1) * wlen);

      wcsncpy(pVar->wpz, (wchar_t *)pz, wlen);
      pVar->nLen = wlen;
      pVar->wpz[wlen] = 0;

      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      pVar->pz = malloc(len + 1);
      strncpy(pVar->pz, pz, len);
      pVar->nLen = len;
      pVar->pz[len] = 0;

      strdequote(pVar->pz);
      pVar->nLen = strlen(pVar->pz);

      break;
    }
  }

  pVar->nType = type;
}
void tVariantDestroy(tVariant *pVar) {
  if (pVar == NULL) return;

  if ((pVar->nType == TSDB_DATA_TYPE_BINARY || pVar->nType == TSDB_DATA_TYPE_NCHAR) && pVar->nLen > 0) {
    free(pVar->pz);
    pVar->pz = NULL;
    pVar->nLen = 0;
  }
}

void tVariantAssign(tVariant *pDst, tVariant *pSrc) {
  if (pSrc == NULL || pDst == NULL) return;

  *pDst = *pSrc;

  if (pSrc->nType == TSDB_DATA_TYPE_BINARY || pSrc->nType == TSDB_DATA_TYPE_NCHAR) {
    int32_t len = pSrc->nLen + 1;
    if (pSrc->nType == TSDB_DATA_TYPE_NCHAR) {
      len = len * TSDB_NCHAR_SIZE;
    }

    pDst->pz = calloc(1, len);
    memcpy(pDst->pz, pSrc->pz, len);
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
      taosUcs4ToMbs(pVar->wpz, (wcslen(pVar->wpz) + 1) * TSDB_NCHAR_SIZE, dst + 1);
      int32_t len = strlen(dst);
      dst[len] = '\'';
      dst[len + 1] = 0;
      return len + 1;
    }

    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
      return sprintf(dst, "%d", (int32_t)pVar->i64Key);

    case TSDB_DATA_TYPE_BIGINT:
      return sprintf(dst, "%ld", pVar->i64Key);

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return sprintf(dst, "%.9lf", pVar->dKey);

    default:
      return 0;
  }
}

static int32_t doConvertToInteger(tVariant *pVariant, char *pDest, int32_t type, bool releaseVariantPtr) {
  if (pVariant->nType >= TSDB_DATA_TYPE_BOOL && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
    *((int64_t *)pDest) = pVariant->i64Key;

  } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
    if ((pVariant->dKey < INT64_MIN) || (pVariant->dKey > INT64_MAX)) {
      return -1;
    }

    *((int64_t *)pDest) = (int64_t)pVariant->dKey;
  } else if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
    errno = 0;
    char *endPtr = NULL;

    SSQLToken token = {0};
    token.n = tSQLGetToken(pVariant->pz, &token.type);

    if (token.type == TK_MINUS || token.type == TK_PLUS) {
      token.n = tSQLGetToken(pVariant->pz + token.n, &token.type);
    }

    if (token.type == TK_FLOAT) {
      double v = strtod(pVariant->pz, &endPtr);
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }

      if ((errno == ERANGE && v == -1) || (isinf(v) || isnan(v))) {
        return -1;
      }

      if ((v < INT64_MIN) || (v > INT64_MAX)) {
        return -1;
      }

      *((int64_t *)pDest) = (int64_t)v;
    } else if (token.type == TK_INTEGER) {
      int64_t val = strtoll(pVariant->pz, &endPtr, 10);
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }

      if (errno == ERANGE) {
        return -1;  // data overflow
      }

      *((int64_t *)pDest) = val;
    } else if (strncasecmp(TSDB_DATA_NULL_STR_L, pVariant->pz, pVariant->nLen) == 0 &&
               strlen(TSDB_DATA_NULL_STR_L) == pVariant->nLen) {
      setNull(pDest, type, tDataTypeDesc[type].nSize);
    } else {
      return -1;
    }

  } else if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
    errno = 0;
    wchar_t *endPtr = NULL;

    SSQLToken token = {0};
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

      if ((v < INT64_MIN) || (v > INT64_MAX)) {
        return -1;
      }

      *((int64_t *)pDest) = (int64_t)v;
    } else {
      int64_t val = wcstoll(pVariant->wpz, &endPtr, 10);
      if (releaseVariantPtr) {
        free(pVariant->pz);
        pVariant->nLen = 0;
      }

      if (errno == ERANGE) {
        return -1;  // data overflow
      }

      *((int64_t *)pDest) = val;
    }
  }

  return 0;
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
  } else {
    return -1;
  }
}

static int32_t toBinary(tVariant *pVariant, char **pDest, int32_t *pDestSize) {
  const int32_t INITIAL_ALLOC_SIZE = 20;
  char *        pBuf = NULL;

  if (*pDest == pVariant->pz) {
    pBuf = calloc(1, INITIAL_ALLOC_SIZE);
  }

  if (pVariant->nType == TSDB_DATA_TYPE_NCHAR) {
    size_t newSize = pVariant->nLen * TSDB_NCHAR_SIZE;
    if (pBuf != NULL) {
      if (newSize > INITIAL_ALLOC_SIZE) {
        pBuf = realloc(pBuf, newSize + 1);
      }

      taosUcs4ToMbs(pVariant->wpz, newSize, pBuf);
      free(pVariant->wpz);

      /* terminated string */
      pBuf[newSize] = 0;
    } else {
      taosUcs4ToMbs(pVariant->wpz, newSize, *pDest);
    }

  } else {
    if (pVariant->nType >= TSDB_DATA_TYPE_TINYINT && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%ld", pVariant->i64Key);
    } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%lf", pVariant->dKey);
    } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
      sprintf(pBuf == NULL ? *pDest : pBuf, "%s", (pVariant->i64Key == TSDB_TRUE) ? "TRUE" : "FALSE");
    }
  }

  if (pBuf != NULL) {
    *pDest = pBuf;
  }
  *pDestSize = strlen(*pDest);
  return 0;
}

static int32_t toNchar(tVariant *pVariant, char **pDest, int32_t *pDestSize) {
  char tmpBuf[40] = {0};

  char *  pDst = tmpBuf;
  int32_t nLen = 0;

  if (pVariant->nType >= TSDB_DATA_TYPE_TINYINT && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
    nLen = sprintf(pDst, "%ld", pVariant->i64Key);
  } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
    nLen = sprintf(pDst, "%lf", pVariant->dKey);
  } else if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
    pDst = pVariant->pz;
    nLen = pVariant->nLen;
  } else if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
    nLen = sprintf(pDst, "%s", (pVariant->i64Key == TSDB_TRUE) ? "TRUE" : "FALSE");
  }

  if (*pDest == pVariant->pz) {
    wchar_t *pWStr = calloc(1, (nLen + 1) * TSDB_NCHAR_SIZE);
    taosMbsToUcs4(pDst, nLen, (char *)pWStr, (nLen + 1) * TSDB_NCHAR_SIZE);

    if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
      free(pVariant->wpz);
    }
    pVariant->wpz = pWStr;

    *pDestSize = wcslen(pVariant->wpz);
  } else {
    taosMbsToUcs4(pDst, nLen, *pDest, (nLen + 1) * TSDB_NCHAR_SIZE);
  }

  return 0;
}

static FORCE_INLINE int32_t convertToInteger(tVariant *pVariant, int64_t *result, int32_t type, int64_t lowBnd,
                                             int64_t upperBnd) {
  if (doConvertToInteger(pVariant, (char *)result, type, false) != 0) {
    return -1;
  }

  if (isNull((char *)result, type)) {
    return 0;
  }

  if (*result < lowBnd || *result > upperBnd) {
    return -1;
  }

  return 0;
}

static int32_t convertToBool(tVariant *pVariant, int64_t *pDest) {
  if (pVariant->nType == TSDB_DATA_TYPE_BOOL) {
    *pDest = pVariant->i64Key;  // in order to be compatible to null of bool
  } else if (pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
    *pDest = ((pVariant->i64Key != 0) ? TSDB_TRUE : TSDB_FALSE);
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
  }

  assert(*pDest == TSDB_TRUE || *pDest == TSDB_FALSE || *pDest == TSDB_DATA_BOOL_NULL);
  return 0;
}

/*
 * transfer data from variant serve as the implicit data conversion: from input sql string pVariant->nType
 * to column type defined in schema
 *
 * todo handle the return value
 */
int32_t tVariantDump(tVariant *pVariant, char *payload, char type) {
  if (pVariant == NULL || !isValidDataType(pVariant->nType, pVariant->nLen)) {  // value is not set
    return -1;
  }

  char *endPtr = NULL;
  errno = 0;  // reset global error code

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      int64_t dst = 0;
      if (convertToBool(pVariant, &dst) < 0) {
        return -1;
      }
      *(int8_t *)payload = (int8_t)dst;
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int64_t result = 0;
      if (convertToInteger(pVariant, &result, type, INT8_MIN, INT8_MAX) < 0) {
        return -1;
      }

      *((int8_t *)payload) = (int8_t)result;
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int64_t result = 0;
      if (convertToInteger(pVariant, &result, type, INT16_MIN, INT16_MAX) < 0) {
        return -1;
      }

      *((int16_t *)payload) = (int16_t)result;
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int64_t result = 0;
      if (convertToInteger(pVariant, &result, type, INT32_MIN, INT32_MAX) < 0) {
        return -1;
      }

      *((int32_t *)payload) = (int32_t)result;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t result = 0;
      if (doConvertToInteger(pVariant, (char *)&result, type, false) != 0) {
        return -1;
      }

      *((int64_t *)payload) = (int64_t)result;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
        if (strncasecmp(TSDB_DATA_NULL_STR_L, pVariant->pz, pVariant->nLen) == 0 &&
            strlen(TSDB_DATA_NULL_STR_L) == pVariant->nLen) {
          setNull(payload, type, tDataTypeDesc[type].nSize);
        } else {
          *((float *)payload) = (float)strtod(pVariant->pz, &endPtr);
          if (errno == ERANGE && *(float *)payload == -1) {
            return -1;
          }

          if (isinf(*(float *)payload) || isnan(*(float *)payload)) {
            return -1;
          }
        }
      } else if (pVariant->nType >= TSDB_DATA_TYPE_BOOL && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
        *((float *)payload) = pVariant->i64Key;
      } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
        *((float *)payload) = (float)pVariant->dKey;

        if (isinf(pVariant->dKey) || isnan(pVariant->dKey) || pVariant->dKey > FLT_MAX || pVariant->dKey < -FLT_MAX) {
          return -1;
        }
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      if (pVariant->nType == TSDB_DATA_TYPE_BINARY) {
        if (strncasecmp(TSDB_DATA_NULL_STR_L, pVariant->pz, pVariant->nLen) == 0 &&
            strlen(TSDB_DATA_NULL_STR_L) == pVariant->nLen) {
          setNull(payload, type, tDataTypeDesc[type].nSize);
        } else {
          *((double *)payload) = strtod(pVariant->pz, &endPtr);
          if (errno == ERANGE && *(double *)payload == -1) {
            return -1;
          }

          if (isnan(*(double *)payload) || isinf(*(double *)payload)) {
            return -1;
          }
        }
      } else if (pVariant->nType >= TSDB_DATA_TYPE_BOOL && pVariant->nType <= TSDB_DATA_TYPE_BIGINT) {
        *((double *)payload) = pVariant->i64Key;
      } else if (pVariant->nType == TSDB_DATA_TYPE_DOUBLE || pVariant->nType == TSDB_DATA_TYPE_FLOAT) {
        *((double *)payload) = pVariant->dKey;
        if (isnan(pVariant->dKey) || isinf(pVariant->dKey)) {
          return -1;
        }
      }

      break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      if (pVariant->nType != TSDB_DATA_TYPE_BINARY) {
        toBinary(pVariant, &payload, &pVariant->nLen);
      } else {
        strncpy(payload, pVariant->pz, pVariant->nLen);
      }
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      *((int64_t *)payload) = pVariant->i64Key;
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      if (pVariant->nType != TSDB_DATA_TYPE_NCHAR) {
        toNchar(pVariant, &payload, &pVariant->nLen);
      } else {
        wcsncpy((wchar_t *)payload, pVariant->wpz, pVariant->nLen);
      }
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
      if (convertToBool(pVariant, &pVariant->i64Key) < 0) {
        return -1;
      }

      pVariant->nType = type;
      break;
    }
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      doConvertToInteger(pVariant, (char *)&pVariant->i64Key, type, true);
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
        pVariant->dKey = pVariant->i64Key;
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
        toNchar(pVariant, &pVariant->pz, &pVariant->nLen);
      }
      pVariant->nType = type;
      break;
    }
  }

  return 0;
}

bool isNull(const char *val, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return *(uint8_t *)val == TSDB_DATA_BOOL_NULL;
    case TSDB_DATA_TYPE_TINYINT:
      return *(uint8_t *)val == TSDB_DATA_TINYINT_NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      return *(uint16_t *)val == TSDB_DATA_SMALLINT_NULL;
    case TSDB_DATA_TYPE_INT:
      return *(uint32_t *)val == TSDB_DATA_INT_NULL;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      return *(uint64_t *)val == TSDB_DATA_BIGINT_NULL;
    case TSDB_DATA_TYPE_FLOAT:
      return *(uint32_t *)val == TSDB_DATA_FLOAT_NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      return *(uint64_t *)val == TSDB_DATA_DOUBLE_NULL;
    case TSDB_DATA_TYPE_NCHAR:
      return *(uint32_t *)val == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return *(uint8_t *)val == TSDB_DATA_BINARY_NULL;
    default:
      return false;
  };
}

void setNull(char *val, int32_t type, int32_t bytes) { setNullN(val, type, bytes, 1); }

void setNullN(char *val, int32_t type, int32_t bytes, int32_t numOfElems) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_BOOL_NULL;
      }
      break;
    case TSDB_DATA_TYPE_TINYINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_TINYINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint16_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_SMALLINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_INT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_INT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_BIGINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_FLOAT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_FLOAT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_DOUBLE_NULL;
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * bytes) = TSDB_DATA_NCHAR_NULL;
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * bytes) = TSDB_DATA_BINARY_NULL;
      }
      break;
    default: {
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize) = TSDB_DATA_INT_NULL;
      }
      break;
    }
  }
}

void assignVal(char *val, char *src, int32_t len, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *((int32_t *)val) = *(int32_t *)src;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *((float *)val) = *(float *)src;
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      *((double *)val) = *(double *)src;
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *((int64_t *)val) = *(int64_t *)src;
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *((int16_t *)val) = *(int16_t *)src;
      break;
    };
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      *((int8_t *)val) = *(int8_t *)src;
      break;
    };
    default: {
      memcpy(val, src, len);
      break;
    }
  }
}

void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size) {
  char tmpBuf[4096] = {0};

  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      SWAP(*(int32_t *)(pLeft), *(int32_t *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      SWAP(*(int64_t *)(pLeft), *(int64_t *)(pRight));
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      SWAP(*(double *)(pLeft), *(double *)(pRight));
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      SWAP(*(int16_t *)(pLeft), *(int16_t *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      SWAP(*(float *)(pLeft), *(float *)(pRight));
      break;
    }

    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      SWAP(*(int8_t *)(pLeft), *(int8_t *)(pRight));
      break;
    }

    default: {
      assert(size <= 4096);
      memcpy(tmpBuf, pLeft, size);
      memcpy(pLeft, pRight, size);
      memcpy(pRight, tmpBuf, size);
      break;
    }
  }
}
