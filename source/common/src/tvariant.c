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

int32_t toInteger(const char *z, int32_t n, int32_t base, int64_t *value) {
  errno = 0;
  char *endPtr = NULL;

  *value = taosStr2Int64(z, &endPtr, base);
  if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
    errno = 0;
    return -1;
  }

  return 0;
}

int32_t toUInteger(const char *z, int32_t n, int32_t base, uint64_t *value) {
  errno = 0;
  char *endPtr = NULL;

  const char *p = z;
  while (*p == ' ') p++;
  if (*p == '-') {
    return -1;
  }

  *value = taosStr2UInt64(z, &endPtr, base);
  if (errno == ERANGE || errno == EINVAL || endPtr - z != n) {
    errno = 0;
    return -1;
  }

  return 0;
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
      pVar->nType == TSDB_DATA_TYPE_JSON || pVar->nType == TSDB_DATA_TYPE_GEOMETRY) {
    taosMemoryFreeClear(pVar->pz);
    pVar->nLen = 0;
  }

}

void taosVariantAssign(SVariant *pDst, const SVariant *pSrc) {
  if (pSrc == NULL || pDst == NULL) return;

  pDst->nType = pSrc->nType;
  if (pSrc->nType == TSDB_DATA_TYPE_BINARY || pSrc->nType == TSDB_DATA_TYPE_NCHAR ||
      pSrc->nType == TSDB_DATA_TYPE_JSON || pSrc->nType == TSDB_DATA_TYPE_GEOMETRY) {
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

  if (p1->nType == TSDB_DATA_TYPE_BINARY || p1->nType == TSDB_DATA_TYPE_NCHAR || p1->nType == TSDB_DATA_TYPE_GEOMETRY) {
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
