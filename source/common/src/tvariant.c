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

#define _DEFAULT_SOURCE
#include "tvariant.h"
#include "ttime.h"
#include "ttokendef.h"
#include "tvariant.h"

int32_t parseBinaryUInteger(const char *z, int32_t n, uint64_t *value) {
  // skip head 0b
  const char *p = z + 2;
  int32_t     l = n - 2;

  while (*p == '0' && l > 0) {
    p++;
    l--;
  }
  if (l > 64) {  // too big
    return TSDB_CODE_FAILED;
  }

  uint64_t val = 0;
  for (int32_t i = 0; i < l; i++) {
    val = val << 1;
    if (p[i] == '1') {
      val |= 1;
    } else if (p[i] != '0') {  // abnormal char
      return TSDB_CODE_FAILED;
    }
  }
  *value = val;
  return TSDB_CODE_SUCCESS;
}

int32_t parseSignAndUInteger(const char *z, int32_t n, bool *is_neg, uint64_t *value) {
  // parse sign
  bool has_sign = false;
  if (z[0] == '-') {
    *is_neg = true;
    has_sign = true;
  } else if (z[0] == '+') {
    has_sign = true;
  } else if (z[0] != '.' && (z[0] < '0' || z[0] > '9')) {
    return TSDB_CODE_FAILED;
  }
  if (has_sign) {
    if (n < 2) {
      return TSDB_CODE_FAILED;
    }
    z++;
    n--;
  }

  errno = 0;
  char  *endPtr = NULL;
  bool parsed = false;
  if (z[0] == '0' && n > 2) {
    if (z[1] == 'b' || z[1] == 'B') {
      // paring as binary
      return parseBinaryUInteger(z, n, value);
    }

    if (z[1] == 'x' || z[1] == 'X') {
      // parsing as hex
      *value = taosStr2UInt64(z, &endPtr, 16);
      parsed = true;
    }
  }

  if (!parsed) {
    // parsing as double
    double val = taosStr2Double(z, &endPtr);
    if (val > UINT64_MAX) {
      return TSDB_CODE_FAILED;
    }
    *value = round(val);
  }

  if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}


int32_t toDoubleEx(const char *z, int32_t n, uint32_t type, double* value) {
  if (n == 0) {
    *value = 0;
    return TSDB_CODE_SUCCESS;
  }

  errno = 0;
  char* endPtr = NULL;
  *value = taosStr2Double(z, &endPtr);

  if (errno == ERANGE || errno == EINVAL) {
    return TSDB_CODE_FAILED;
  }

  if (endPtr - z != n) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t toIntegerEx(const char *z, int32_t n, uint32_t type, int64_t *value) {
  errno = 0;
  char *endPtr = NULL;
  switch (type)
  {
    case TK_NK_INTEGER: {
      *value = taosStr2Int64(z, &endPtr, 10);
      if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
        return TSDB_CODE_FAILED;
      }
      return TSDB_CODE_SUCCESS;
    } break;
    case TK_NK_FLOAT: {
      double val = round(taosStr2Double(z, &endPtr));
      if (!IS_VALID_INT64(val)) {
        return TSDB_CODE_FAILED;
      }
      if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
        return TSDB_CODE_FAILED;
      }
      *value = val;
      return TSDB_CODE_SUCCESS;
    } break;
  default:
    break;
  }

  if (n == 0) {
    *value = 0;
    return TSDB_CODE_SUCCESS;
  }

  // 1. try to parse as integer
  *value = taosStr2Int64(z, &endPtr, 10);
  if (endPtr - z == n) {
    if (errno == ERANGE || errno == EINVAL) {
      return TSDB_CODE_FAILED;
    }
    return TSDB_CODE_SUCCESS;
  } else if (errno == 0 && *endPtr == '.') {
    // pure decimal part
    const char *s = endPtr + 1;
    const char *end = z + n;
    bool pure = true;
    while (s < end) {
      if (*s < '0' || *s > '9') {
        pure = false;
        break;
      }
      s++;
    }
    if (pure) {
      if (endPtr+1 < end && endPtr[1] > '4') {
        const char *p = z;
        while (*p == ' ') {
          p++;
        }
        if (*p == '-') {
          if ( *value > INT64_MIN) {
            (*value)--;
          }
        } else {
          if ( *value < INT64_MAX) {
            (*value)++;
          }
        }
      }
      return TSDB_CODE_SUCCESS;
    }
  }

  // 2. parse as other 
  bool     is_neg = false;
  uint64_t uv = 0;
  int32_t  code = parseSignAndUInteger(z, n, &is_neg, &uv);
  if (code == TSDB_CODE_SUCCESS) {
    // truncate into int64
    if (is_neg) {
      if (uv > 1ull + INT64_MAX) {
        *value = INT64_MIN;
        return TSDB_CODE_FAILED;
      } else {
        *value = -uv;
      }
    } else {
      if (uv > INT64_MAX) {
        *value = INT64_MAX;
        return TSDB_CODE_FAILED;
      }
      *value = uv;
    }
  }

  return code;
}

int32_t toUIntegerEx(const char *z, int32_t n, uint32_t type, uint64_t *value) {
  errno = 0;
  char *endPtr = NULL;
  const char *p = z;
  while (*p == ' ') {
    p++;
  }
  switch (type) {
    case TK_NK_INTEGER: {
      *value = taosStr2UInt64(p, &endPtr, 10);
      if (*p == '-' && *value) {
        return TSDB_CODE_FAILED;
      }
      if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
        return TSDB_CODE_FAILED;
      }
      return TSDB_CODE_SUCCESS;
    } break;
    case TK_NK_FLOAT: {
      double val = round(taosStr2Double(p, &endPtr));
      if (!IS_VALID_UINT64(val)) {
        return TSDB_CODE_FAILED;
      }
      if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
        return TSDB_CODE_FAILED;
      }
      *value = val;
      return TSDB_CODE_SUCCESS;
    } break;
    default:
      break;
  }

  if (n == 0) {
    *value = 0;
    return TSDB_CODE_SUCCESS;
  }

  // 1. parse as integer
  *value = taosStr2UInt64(p, &endPtr, 10);
  if (*p == '-' && *value) {
    return TSDB_CODE_FAILED;
  }
  if (endPtr - z == n) {
    if (errno == ERANGE || errno == EINVAL) {
      return TSDB_CODE_FAILED;
    }
    return TSDB_CODE_SUCCESS;
  } else if (errno == 0 && *endPtr == '.') {
    const char *s = endPtr + 1;
    const char *end = z + n;
    bool pure = true;
    while (s < end) {
      if (*s < '0' || *s > '9') {
        pure = false;
        break;
      }
      s++;
    }
    if (pure) {
      if (endPtr + 1 < end && endPtr[1] > '4' && *value < UINT64_MAX) {
        (*value)++;
      }
      return TSDB_CODE_SUCCESS;
    }
  }

  // 2. parse as other 
  bool    is_neg = false;
  int32_t code = parseSignAndUInteger(z, n, &is_neg, value);
  if (is_neg) {
    if (TSDB_CODE_SUCCESS == code && 0 == *value) {
      return TSDB_CODE_SUCCESS;
    }
    return TSDB_CODE_FAILED;
  }
  return code;
}

int32_t toInteger(const char *z, int32_t n, int32_t base, int64_t *value) {
  errno = 0;
  char *endPtr = NULL;

  *value = taosStr2Int64(z, &endPtr, base);
  if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
    errno = 0;
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t toUInteger(const char *z, int32_t n, int32_t base, uint64_t *value) {
  errno = 0;
  char *endPtr = NULL;

  const char *p = z;
  while (*p == ' ') p++;
  if (*p == '-') {
    return TSDB_CODE_FAILED;
  }

  *value = taosStr2UInt64(z, &endPtr, base);
  if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
    errno = 0;
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * create SVariant from binary string, not ascii data
 * @param pVar
 * @param pz
 * @param len
 * @param type
 */
void taosVariantCreateFromBinary(SVariant *pVar, const char *pz, size_t len, uint32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->i = GET_INT8_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->u = GET_UINT8_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->i = GET_INT16_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->u = GET_UINT16_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->i = GET_INT32_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->u = GET_UINT32_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->i = GET_INT64_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->u = GET_UINT64_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->d = GET_DOUBLE_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      pVar->nLen = tDataTypes[type].bytes;
      pVar->f = GET_FLOAT_VAL(pz);
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {  // here we get the nchar length from raw binary bits length
      size_t lenInwchar = len / TSDB_NCHAR_SIZE;

      pVar->ucs4 = taosMemoryCalloc(1, (lenInwchar + 1) * TSDB_NCHAR_SIZE);
      memcpy(pVar->ucs4, pz, lenInwchar * TSDB_NCHAR_SIZE);
      pVar->nLen = (int32_t)len;

      break;
    }
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {  // todo refactor, extract a method
      pVar->pz = taosMemoryCalloc(len + 1, sizeof(char));
      memcpy(pVar->pz, pz, len);
      pVar->nLen = (int32_t)len;
      break;
    }

    default:
      pVar->i = GET_INT32_VAL(pz);
      pVar->nLen = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  }

  pVar->nType = type;
}

void taosVariantDestroy(SVariant *pVar) {
  if (pVar == NULL) return;

  if (pVar->nType == TSDB_DATA_TYPE_BINARY || pVar->nType == TSDB_DATA_TYPE_NCHAR ||
      pVar->nType == TSDB_DATA_TYPE_JSON || pVar->nType == TSDB_DATA_TYPE_GEOMETRY ||
      pVar->nType == TSDB_DATA_TYPE_VARBINARY) {
    taosMemoryFreeClear(pVar->pz);
    pVar->nLen = 0;
  }
}

void taosVariantAssign(SVariant *pDst, const SVariant *pSrc) {
  if (pSrc == NULL || pDst == NULL) return;

  pDst->nType = pSrc->nType;
  if (pSrc->nType == TSDB_DATA_TYPE_BINARY || pSrc->nType == TSDB_DATA_TYPE_VARBINARY ||
      pSrc->nType == TSDB_DATA_TYPE_NCHAR || pSrc->nType == TSDB_DATA_TYPE_JSON ||
      pSrc->nType == TSDB_DATA_TYPE_GEOMETRY) {
    int32_t len = pSrc->nLen + TSDB_NCHAR_SIZE;
    char   *p = taosMemoryRealloc(pDst->pz, len);
    ASSERT(p);

    memset(p, 0, len);
    pDst->pz = p;

    memcpy(pDst->pz, pSrc->pz, pSrc->nLen);
    pDst->nLen = pSrc->nLen;
    return;
  }

  if (IS_NUMERIC_TYPE(pSrc->nType) || (pSrc->nType == TSDB_DATA_TYPE_BOOL)) {
    pDst->i = pSrc->i;
  }
}

int32_t taosVariantCompare(const SVariant *p1, const SVariant *p2) {
  if (p1->nType == TSDB_DATA_TYPE_NULL && p2->nType == TSDB_DATA_TYPE_NULL) {
    return 0;
  }

  if (p1->nType == TSDB_DATA_TYPE_NULL) {
    return -1;
  }

  if (p2->nType == TSDB_DATA_TYPE_NULL) {
    return 1;
  }

  if (p1->nType == TSDB_DATA_TYPE_BINARY || p1->nType == TSDB_DATA_TYPE_VARBINARY ||
      p1->nType == TSDB_DATA_TYPE_NCHAR || p1->nType == TSDB_DATA_TYPE_GEOMETRY) {
    if (p1->nLen == p2->nLen) {
      return memcmp(p1->pz, p2->pz, p1->nLen);
    } else {
      return p1->nLen > p2->nLen ? 1 : -1;
    }
  } else if (p1->nType == TSDB_DATA_TYPE_DOUBLE) {
    if (p1->d == p2->d) {
      return 0;
    } else {
      return p1->d > p2->d ? 1 : -1;
    }
  } else if (p1->nType == TSDB_DATA_TYPE_FLOAT) {
    if (p1->f == p2->f) {
      return 0;
    } else {
      return p1->f > p2->f ? 1 : -1;
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(p1->nType)) {
    if (p1->u == p2->u) {
      return 0;
    } else {
      return p1->u > p2->u ? 1 : -1;
    }
  } else {
    if (p1->i == p2->i) {
      return 0;
    } else {
      return p1->i > p2->i ? 1 : -1;
    }
  }
}

char *taosVariantGet(SVariant *pVar, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return (char *)&pVar->i;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return (char *)&pVar->u;
    case TSDB_DATA_TYPE_DOUBLE:
      return (char *)&pVar->d;
    case TSDB_DATA_TYPE_FLOAT:
      return (char *)&pVar->f;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      return (char *)pVar->pz;
    case TSDB_DATA_TYPE_NCHAR:
      return (char *)pVar->ucs4;
    default:
      return NULL;
  }

  return NULL;
}
