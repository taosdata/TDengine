/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
// System locale headers must precede the OS abstraction's forbidden-API macros.
#ifndef WINDOWS
#include <langinfo.h>
#include <locale.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif
#endif
#include "tcompare.h"
#include "tconv.h"
#include "decimal.h"
#include "osString.h"
#include "regex.h"
#include "tdef.h"
#include "thash.h"
#include "tlog.h"
#include "ttimer.h"
#include "tutil.h"
#include "types.h"
#if defined(WINDOWS) && defined(USE_PCRE2)
#include "tpcre2.h"
#endif

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

int32_t setChkInDecimalHash(const void *pLeft, const void *pRight) {
  const SDecimalCompareCtx *pCtxL = pLeft, *pCtxR = pRight;
  return NULL != taosHashGet((SHashObj *)(pCtxR->pData), pCtxL->pData, tDataTypes[pCtxL->type].bytes) ? 1 : 0;
}

int32_t setChkNotInDecimalHash(const void *pLeft, const void *pRight) {
  const SDecimalCompareCtx *pCtxL = pLeft, *pCtxR = pRight;
  return NULL == taosHashGet((SHashObj *)(pCtxR->pData), pCtxL->pData, tDataTypes[pCtxL->type].bytes) ? 1 : 0;
}

int32_t compareChkInString(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, varDataTLen(pLeft)) ? 1 : 0;
}

int32_t compareChkNotInString(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, varDataTLen(pLeft)) ? 1 : 0;
}

int32_t compareChkInBlob(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, blobDataTLen(pLeft)) ? 1 : 0;
}

int32_t compareChkNotInBlob(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, blobDataTLen(pLeft)) ? 1 : 0;
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
  int64_t left = GET_INT64_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  return (left > right) - (left < right);
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

  if (DBL_EQUAL(p1, p2)) {
    return 0;
  }
  return DBL_GREATER(p1, p2) ? 1 : -1;
}

int32_t compareDoubleValDesc(const void *pLeft, const void *pRight) { return compareDoubleVal(pRight, pLeft); }

int32_t compareLenPrefixedStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  int32_t minLen = TMIN(len1, len2);
  int32_t ret = strncmp(varDataVal(pLeft), varDataVal(pRight), minLen);
  if (ret == 0) {
    if (len1 == len2) {
      return 0;
    } else {
      return len1 > len2 ? 1 : -1;
    }
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareLenPrefixedStrDesc(const void *pLeft, const void *pRight) {
  return compareLenPrefixedStr(pRight, pLeft);
}

int32_t compareLenPrefixedWStr(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);
  int32_t ret = taosUcs4Compare((TdUcs4 *)varDataVal(pLeft), (TdUcs4 *)varDataVal(pRight), len1 > len2 ? len2 : len1);

/*  
  int32_t bufSize = 256;
  char *buf1 = NULL;
  char *buf2 = NULL;
  do {
    buf1 = taosMemoryCalloc(1, bufSize);
    if (buf1 == NULL) {
      break;
    }
    buf2 = taosMemoryCalloc(1, bufSize);
    if (buf2 == NULL) {
      break;
    }
    int32_t code = taosHexEncode(varDataVal(pLeft), buf1, varDataLen(pLeft), bufSize);
    if (code) {
      break;
    }
    code = taosHexEncode(varDataVal(pRight), buf2, varDataLen(pRight), bufSize);
    if (code) {
      break;
    }
    
    uInfo("ucs4compare res:%d, left:%d,%p,[%s], right:%d,%p,[%s]", ret, len1, pLeft, buf1, len2, pRight, buf2);
  } while (0);
  taosMemoryFree(buf1);
  taosMemoryFree(buf2);
*/

  if (ret == 0) {
    if (len1 > len2)
      return 1;
    else if (len1 < len2)
      return -1;
    else
      return 0;
  }
  return (ret < 0) ? -1 : 1;
}

int32_t compareLenPrefixedWStrDesc(const void *pLeft, const void *pRight) {
  return compareLenPrefixedWStr(pRight, pLeft);
}

int32_t compareLenBinaryVal(const void *pLeft, const void *pRight) {
  int32_t len1 = varDataLen(pLeft);
  int32_t len2 = varDataLen(pRight);

  int32_t minLen = TMIN(len1, len2);
  int32_t ret = memcmp(varDataVal(pLeft), varDataVal(pRight), minLen);
  if (ret == 0) {
    if (len1 == len2) {
      return 0;
    } else {
      return len1 > len2 ? 1 : -1;
    }
  } else {
    return ret > 0 ? 1 : -1;
  }
}
int32_t compareBlobVal(const void *pLeft, const void *pRight) {
  int32_t len1 = blobDataLen(pLeft);
  int32_t len2 = blobDataLen(pRight);

  int32_t minLen = TMIN(len1, len2);
  int32_t ret = memcmp(blobDataVal(pLeft), blobDataVal(pRight), minLen);
  if (ret == 0) {
    if (len1 == len2) {
      return 0;
    } else {
      return len1 > len2 ? 1 : -1;
    }
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareLenBinaryValDesc(const void *pLeft, const void *pRight) { return compareLenBinaryVal(pRight, pLeft); }
int32_t compareBlobValDesc(const void *pLeft, const void *pRight) { return compareBlobVal(pRight, pLeft); }

// string > number > bool > null
// ref: https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison
int32_t compareJsonVal(const void *pLeft, const void *pRight) {
  char leftType = *(char *)pLeft;
  char rightType = *(char *)pRight;
  if (leftType != rightType) {
    return leftType > rightType ? 1 : -1;
  }

  char *realDataLeft = POINTER_SHIFT(pLeft, CHAR_BYTES);
  char *realDataRight = POINTER_SHIFT(pRight, CHAR_BYTES);
  if (leftType == TSDB_DATA_TYPE_BOOL) {
    DEFAULT_COMP(GET_INT8_VAL(realDataLeft), GET_INT8_VAL(realDataRight));
  } else if (leftType == TSDB_DATA_TYPE_DOUBLE) {
    DEFAULT_DOUBLE_COMP(GET_DOUBLE_VAL(realDataLeft), GET_DOUBLE_VAL(realDataRight));
  } else if (leftType == TSDB_DATA_TYPE_NCHAR) {
    return compareLenPrefixedWStr(realDataLeft, realDataRight);
  } else if (leftType == TSDB_DATA_TYPE_NULL) {
    return 0;
  } else {
    uError("data type unexpected leftType:%d rightType:%d", leftType, rightType);
    return 0;
  }
}

int32_t compareInt8Int16(const void *pLeft, const void *pRight) {
  int8_t  left = GET_INT8_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Int32(const void *pLeft, const void *pRight) {
  int8_t  left = GET_INT8_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Int64(const void *pLeft, const void *pRight) {
  int8_t  left = GET_INT8_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Float(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft);
  float  right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Double(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft);
  double right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Uint8(const void *pLeft, const void *pRight) {
  int8_t  left = GET_INT8_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Uint16(const void *pLeft, const void *pRight) {
  int8_t   left = GET_INT8_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt8Uint32(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt8Uint64(const void *pLeft, const void *pRight) {
  int8_t left = GET_INT8_VAL(pLeft);
  if (left < 0) return -1;
  uint64_t right = GET_UINT64_VAL(pRight);
  if ((uint64_t)left > right) return 1;
  if ((uint64_t)left < right) return -1;
  return 0;
}

int32_t compareInt16Int8(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  int8_t  right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Int32(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Int64(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Float(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  float   right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Double(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  double  right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Uint8(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Uint16(const void *pLeft, const void *pRight) {
  int16_t  left = GET_INT16_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt16Uint32(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt16Uint64(const void *pLeft, const void *pRight) {
  int16_t left = GET_INT16_VAL(pLeft);
  if (left < 0) return -1;
  uint64_t right = GET_UINT64_VAL(pRight);
  if ((uint64_t)left > right) return 1;
  if ((uint64_t)left < right) return -1;
  return 0;
}

int32_t compareInt32Int8(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  int8_t  right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Int16(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Int64(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Float(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  float   right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Double(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  double  right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Uint8(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Uint16(const void *pLeft, const void *pRight) {
  int32_t  left = GET_INT32_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt32Uint32(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt32Uint64(const void *pLeft, const void *pRight) {
  int32_t left = GET_INT32_VAL(pLeft);
  if (left < 0) return -1;
  uint64_t right = GET_UINT64_VAL(pRight);
  if ((uint64_t)left > right) return 1;
  if ((uint64_t)left < right) return -1;
  return 0;
}

int32_t compareInt64Int8(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  int8_t  right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Int16(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Int32(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Float(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  float   right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Double(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  double  right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Uint8(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Uint16(const void *pLeft, const void *pRight) {
  int64_t  left = GET_INT64_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Uint32(const void *pLeft, const void *pRight) {
  int64_t  left = GET_INT64_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareInt64Uint64(const void *pLeft, const void *pRight) {
  int64_t left = GET_INT64_VAL(pLeft);
  if (left < 0) return -1;
  uint64_t right = GET_UINT64_VAL(pRight);
  if ((uint64_t)left > right) return 1;
  if ((uint64_t)left < right) return -1;
  return 0;
}

int32_t compareFloatInt8(const void *pLeft, const void *pRight) {
  float  left = GET_FLOAT_VAL(pLeft);
  int8_t right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatInt16(const void *pLeft, const void *pRight) {
  float   left = GET_FLOAT_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatInt32(const void *pLeft, const void *pRight) {
  float   left = GET_FLOAT_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatInt64(const void *pLeft, const void *pRight) {
  float   left = GET_FLOAT_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatDouble(const void *pLeft, const void *pRight) {
  float  left = GET_FLOAT_VAL(pLeft);
  double right = GET_DOUBLE_VAL(pRight);

  if (isnan(left) && isnan(right)) {
    return 0;
  }

  if (isnan(left)) {
    return -1;
  }

  if (isnan(right)) {
    return 1;
  }

  if (FLT_EQUAL(left, right)) {
    return 0;
  }
  return FLT_GREATER(left, right) ? 1 : -1;
}

int32_t compareFloatUint8(const void *pLeft, const void *pRight) {
  float   left = GET_FLOAT_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatUint16(const void *pLeft, const void *pRight) {
  float    left = GET_FLOAT_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatUint32(const void *pLeft, const void *pRight) {
  float    left = GET_FLOAT_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareFloatUint64(const void *pLeft, const void *pRight) {
  float    left = GET_FLOAT_VAL(pLeft);
  uint64_t right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleInt8(const void *pLeft, const void *pRight) {
  double left = GET_DOUBLE_VAL(pLeft);
  int8_t right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleInt16(const void *pLeft, const void *pRight) {
  double  left = GET_DOUBLE_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleInt32(const void *pLeft, const void *pRight) {
  double  left = GET_DOUBLE_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleInt64(const void *pLeft, const void *pRight) {
  double  left = GET_DOUBLE_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleFloat(const void *pLeft, const void *pRight) {
  double left = GET_DOUBLE_VAL(pLeft);
  float  right = GET_FLOAT_VAL(pRight);

  if (isnan(left) && isnan(right)) {
    return 0;
  }

  if (isnan(left)) {
    return -1;
  }

  if (isnan(right)) {
    return 1;
  }

  if (FLT_EQUAL(left, right)) {
    return 0;
  }
  return FLT_GREATER(left, right) ? 1 : -1;
}

int32_t compareDoubleUint8(const void *pLeft, const void *pRight) {
  double  left = GET_DOUBLE_VAL(pLeft);
  uint8_t right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleUint16(const void *pLeft, const void *pRight) {
  double   left = GET_DOUBLE_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleUint32(const void *pLeft, const void *pRight) {
  double   left = GET_DOUBLE_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDoubleUint64(const void *pLeft, const void *pRight) {
  double   left = GET_DOUBLE_VAL(pLeft);
  uint64_t right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Int8(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  int8_t  right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Int16(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  int16_t right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Int32(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  int32_t right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Int64(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  int64_t right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Float(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  float   right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Double(const void *pLeft, const void *pRight) {
  uint8_t left = GET_UINT8_VAL(pLeft);
  double  right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Uint16(const void *pLeft, const void *pRight) {
  uint8_t  left = GET_UINT8_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Uint32(const void *pLeft, const void *pRight) {
  uint8_t  left = GET_UINT8_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint8Uint64(const void *pLeft, const void *pRight) {
  uint8_t  left = GET_UINT8_VAL(pLeft);
  uint64_t right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Int8(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  int8_t   right = GET_INT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Int16(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  int16_t  right = GET_INT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Int32(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  int32_t  right = GET_INT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Int64(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  int64_t  right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Float(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  float    right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Double(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  double   right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Uint8(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  uint8_t  right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Uint32(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint16Uint64(const void *pLeft, const void *pRight) {
  uint16_t left = GET_UINT16_VAL(pLeft);
  uint64_t right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Int8(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  int8_t   right = GET_INT8_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint32_t)right) return 1;
  if (left < (uint32_t)right) return -1;
  return 0;
}

int32_t compareUint32Int16(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  int16_t  right = GET_INT16_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint32_t)right) return 1;
  if (left < (uint32_t)right) return -1;
  return 0;
}

int32_t compareUint32Int32(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  int32_t  right = GET_INT32_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint32_t)right) return 1;
  if (left < (uint32_t)right) return -1;
  return 0;
}

int32_t compareUint32Int64(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  int64_t  right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Float(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  float    right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Double(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  double   right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Uint8(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  uint8_t  right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Uint16(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint32Uint64(const void *pLeft, const void *pRight) {
  uint32_t left = GET_UINT32_VAL(pLeft);
  uint64_t right = GET_UINT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64Int8(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  int8_t   right = GET_INT8_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint64_t)right) return 1;
  if (left < (uint64_t)right) return -1;
  return 0;
}

int32_t compareUint64Int16(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  int16_t  right = GET_INT16_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint64_t)right) return 1;
  if (left < (uint64_t)right) return -1;
  return 0;
}

int32_t compareUint64Int32(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  int32_t  right = GET_INT32_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint64_t)right) return 1;
  if (left < (uint64_t)right) return -1;
  return 0;
}

int32_t compareUint64Int64(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  int64_t  right = GET_INT64_VAL(pRight);
  if (right < 0) return 1;
  if (left > (uint64_t)right) return 1;
  if (left < (uint64_t)right) return -1;
  return 0;
}

int32_t compareUint64Float(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  float    right = GET_FLOAT_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64Double(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  double   right = GET_DOUBLE_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64Uint8(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  uint8_t  right = GET_UINT8_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64Uint16(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  uint16_t right = GET_UINT16_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareUint64Uint32(const void *pLeft, const void *pRight) {
  uint64_t left = GET_UINT64_VAL(pLeft);
  uint32_t right = GET_UINT32_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
}

int32_t compareDecimal64(const void *pleft, const void *pright) {
  if (decimal64Compare(OP_TYPE_GREATER_THAN, pleft, pright)) return 1;
  if (decimal64Compare(OP_TYPE_LOWER_THAN, pleft, pright)) return -1;
  return 0;
}

int32_t compareDecimal128(const void *pleft, const void *pright) {
  if (decimalCompare(OP_TYPE_GREATER_THAN, pleft, pright)) return 1;
  if (decimalCompare(OP_TYPE_LOWER_THAN, pleft, pright)) return -1;
  return 0;
}

int32_t compareDecimal64SameScale(const void *pleft, const void *pright) {
  const SDecimalOps *pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
  if (pOps->gt(pleft, pright, DECIMAL_WORD_NUM(Decimal64))) return 1;
  if (pOps->lt(pleft, pright, DECIMAL_WORD_NUM(Decimal64))) return -1;
  return 0;
}

int32_t compareDecimal64SameScaleDesc(const void *pLeft, const void *pRight) {
  const SDecimalOps *pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
  if (pOps->lt(pLeft, pRight, DECIMAL_WORD_NUM(Decimal64))) return 1;
  if (pOps->gt(pLeft, pRight, DECIMAL_WORD_NUM(Decimal64))) return -1;
  return 0;
}

int32_t compareDecimal128SameScale(const void *pleft, const void *pright) {
  const SDecimalOps *pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  if (pOps->gt(pleft, pright, DECIMAL_WORD_NUM(Decimal))) return 1;
  if (pOps->lt(pleft, pright, DECIMAL_WORD_NUM(Decimal))) return -1;
  return 0;
}

int32_t compareDecimal128SameScaleDesc(const void *pLeft, const void *pRight) {
  const SDecimalOps *pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
  if (pOps->lt(pLeft, pRight, DECIMAL_WORD_NUM(Decimal))) return 1;
  if (pOps->gt(pLeft, pRight, DECIMAL_WORD_NUM(Decimal))) return -1;
  return 0;
}

int32_t compareJsonValDesc(const void *pLeft, const void *pRight) { return compareJsonVal(pRight, pLeft); }

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
int32_t patternMatch(const char *pattern, size_t psize, const char *str, size_t ssize,
                     const SPatternCompareInfo *pInfo) {
  char c, c1;
  int32_t i = 0;
  int32_t j = 0;
  int32_t nMatchChar = 0;

  while ((i < psize) && ((c = pattern[i++]) != 0)) {
    bool escaped = false;

    // handle escaped characters
    if (c == '\\' && i < psize && 
        (pattern[i] == pInfo->matchOne || pattern[i] == pInfo->matchAll || pattern[i] == '\\')) {
      c = pattern[i++];
      escaped = true;
    }

    // handle matchAll wildcard '%' or '*'
    if (!escaped && c == pInfo->matchAll) {
      // skip continues matchAll and matchOne
      while ((i < psize) && ((c = pattern[i++]) == pInfo->matchAll || c == pInfo->matchOne)) {
        if (c == pInfo->matchOne) {
          if (j >= ssize || str[j++] == 0) {
            return TSDB_PATTERN_NOWILDCARDMATCH;
          }
          ++nMatchChar;
        }
      }

      // handle wildcard at the end of the pattern
      if (i >= psize && (c == pInfo->matchOne || c == pInfo->matchAll)) {
        return TSDB_PATTERN_MATCH;
      }

      // now c is the next character after matchAll
      // handle escaped characters (if after wildcard)
      if (c == '\\' && i < psize && 
          (pattern[i] == pInfo->matchOne || pattern[i] == pInfo->matchAll || pattern[i] == '\\')) {
        c = pattern[i++];
      }

      char rejectList[2] = {toupper(c), tolower(c)};
      const char *remainStr = str + nMatchChar;
      int32_t remainSize = ssize - nMatchChar;

      // greedily match the rest of the pattern
      while (1) {
        // find the first occurrence of the reject character in the remaining string
        // this segments the string for potential recursive matching
        size_t n = tstrncspn(remainStr, remainSize, rejectList, 2);
        remainStr += n;
        remainSize -= n;

        if (remainSize <= 0 || remainStr[0] == 0) {
          break;
        }

        int32_t ret = patternMatch(&pattern[i], psize - i, ++remainStr, --remainSize, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    // handle normal character or matchOne wildcard '_'
    if (j < ssize) {
      c1 = str[j++];
      ++nMatchChar;

      if ((c == c1 || tolower(c) == tolower(c1)) || 
          (!escaped && c == pInfo->matchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (j >= ssize || str[j] == 0) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int32_t rawStrPatternMatch(const char *str, const char *pattern) {
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  size_t pLen = strlen(pattern);
  size_t sz = strlen(str);
  if (pLen > TSDB_MAX_FIELD_LEN) {
    return 1;
  }

  int32_t ret = patternMatch(pattern, pLen, str, sz, &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

// same logic with patternMatch, but for UCS4 strings
int32_t wcsPatternMatch(const TdUcs4 *pattern, size_t psize, const TdUcs4 *str, size_t ssize,
                        const SPatternCompareInfo *pInfo) {
  TdUcs4 c, c1;
  int32_t i = 0;
  int32_t j = 0;
  int32_t nMatchChar = 0;

  while ((i < psize) && ((c = pattern[i++]) != 0)) {
    bool escaped = false;

    if (c == '\\' && i < psize && 
        (pattern[i] == pInfo->umatchOne || pattern[i] == pInfo->umatchAll || pattern[i] == '\\')) {
      c = pattern[i++];
      escaped = true;
    }

    if (!escaped && c == pInfo->umatchAll) {
      while ((i < psize) && ((c = pattern[i++]) == pInfo->umatchAll || c == pInfo->umatchOne)) {
        if (c == pInfo->umatchOne) {
          if (j >= ssize || str[j++] == 0) {
            return TSDB_PATTERN_NOWILDCARDMATCH;
          }
          ++nMatchChar;
        }
      }

      if (i >= psize && (c == pInfo->matchOne || c == pInfo->matchAll)) {
        return TSDB_PATTERN_MATCH;
      }

      if (c == '\\' && i < psize && 
          (pattern[i] == pInfo->umatchOne || pattern[i] == pInfo->umatchAll || pattern[i] == '\\')) {
        c = pattern[i++];
      }

      TdUcs4 rejectList[2] = {toupper(c), tolower(c)};
      const TdUcs4 *remainStr = str + nMatchChar;
      int32_t remainSize = ssize - nMatchChar;

      while (1) {
        size_t n = twcsncspn(remainStr, remainSize, rejectList, 2);
        remainStr += n;
        remainSize -= n;

        if (remainSize <= 0 || remainStr[0] == 0) {
          break;
        }

        int32_t ret = wcsPatternMatch(&pattern[i], psize - i, ++remainStr, --remainSize, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    if (j < ssize) {
      c1 = str[j++];
      ++nMatchChar;

      if ((c == c1 || tolower(c) == tolower(c1)) || 
          (!escaped && c == pInfo->umatchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (j >= ssize || str[j] == 0) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

typedef struct UsingRegex {
  regex_t pRegex;
  int32_t lastUsedTime;
} UsingRegex;
typedef UsingRegex *HashRegexPtr;

typedef struct RegexCache {
  SHashObj *regexHash;
  void     *regexCacheTmr;
  void     *timer;
  SRWLatch  mutex;
  bool      exit;
  int8_t    inited;
#ifndef WINDOWS
  SHashObj *poolHash;
  TdThreadMutex poolLock;
  SHashObj *localeHash;
  int32_t poolCount;  // Searchable entries; the hash size also counts retired borrowers.
#endif
} RegexCache;
#ifdef WINDOWS
static RegexCache sRegexCache;
#else
static RegexCache sRegexCache = {.poolLock = PTHREAD_MUTEX_INITIALIZER};
#endif
#define MAX_REGEX_CACHE_SIZE   20
#define REGEX_CACHE_CLEAR_TIME 30

#ifndef WINDOWS
typedef struct SRegexPool {
  regex_t *slots;
  bool *busy;
  int32_t count;
  int32_t compiled;
  int64_t lastUsed;
  locale_t locale;
  TdThreadMutex mutex;
  TdThreadCond available;
} SRegexPool;

static int32_t regexPoolSize = 1;

static void regexLocaleFree(void *value) { freelocale(*(locale_t *)value); }

static void regexPoolFree(void *value) {
  SRegexPool *pool = *(SRegexPool **)value;
  for (int32_t i = 0; i < pool->compiled; ++i) regfree(&pool->slots[i]);
  if (pool->locale != (locale_t)0) freelocale(pool->locale);
  (void)taosThreadCondDestroy(&pool->available);
  (void)taosThreadMutexDestroy(&pool->mutex);
  taosMemoryFree(pool->slots);
  taosMemoryFree(pool->busy);
  taosMemoryFree(pool);
}

static const char *regexCanonicalCharset(const char *charset) {
  // glibc and BSD report different names for the C locale's ASCII codeset.
  if (strcasecmp(charset, "ASCII") == 0 || strcasecmp(charset, "US-ASCII") == 0 ||
      strcasecmp(charset, "ANSI_X3.4-1968") == 0 || strcasecmp(charset, "CP367") == 0 ||
      strcasecmp(charset, "IBM367") == 0 || strcasecmp(charset, "ISO646-US") == 0) {
    return "ASCII";
  }
#ifdef __GLIBC__
  // glibc iconv accepts these aliases, but its locale reports GBK as CODESET.
  if (strcasecmp(charset, "CP936") == 0 || strcasecmp(charset, "MS936") == 0 ||
      strcasecmp(charset, "WINDOWS-936") == 0 || strcasecmp(charset, "GB13000") == 0) {
    return "GBK";
  }
#endif
  return charset;
}

// Locale spelling and charset spelling are separate namespaces. Discover the
// installed locale by CODESET rather than maintaining a list of client encodings.
static bool regexSameCharset(const char *a, const char *b) {
  while (*a || *b) {
    if (*a == '-' || *a == '_') { ++a; continue; }
    if (*b == '-' || *b == '_') { ++b; continue; }
    unsigned char ac = (unsigned char)*a;
    unsigned char bc = (unsigned char)*b;
    if (ac >= 'a' && ac <= 'z') ac -= 'a' - 'A';
    if (bc >= 'a' && bc <= 'z') bc -= 'a' - 'A';
    if (ac != bc) return false;
    if (*a) ++a;
    if (*b) ++b;
  }
  return true;
}

static locale_t regexTryLocale(const char *name, const char *charset) {
  locale_t locale = newlocale(LC_CTYPE_MASK | LC_COLLATE_MASK, name, (locale_t)0);
  if (locale != (locale_t)0 && !regexSameCharset(regexCanonicalCharset(nl_langinfo_l(CODESET, locale)), charset)) {
    freelocale(locale);
    locale = (locale_t)0;
  }
  return locale;
}

static locale_t regexFindLocale(const char *charset) {
  // C/POSIX are always available, but charset names are not locale names.
  // In particular client syntax validation requests ANSI_X3.4-1968. Avoid
  // spawning locale(1) for these built-ins (including under sanitizer preload).
  locale_t locale = regexTryLocale("C", charset);
  if (locale != (locale_t)0) return locale;
  locale = regexTryLocale(tsLocale, charset);
  if (locale != (locale_t)0) return locale;
  locale = regexTryLocale("C.UTF-8", charset);
  if (locale != (locale_t)0) return locale;
  locale = regexTryLocale(charset, charset);
  if (locale != (locale_t)0) return locale;
  // Honour private locale installations as well as the system locale archive.
  const char *paths = getenv("LOCPATH");
  if (paths != NULL) {
    char *copy = taosStrdup(paths);
    if (copy == NULL) return (locale_t)0;
    char *save = NULL;
    for (char *path = strtok_r(copy, ":", &save); path != NULL; path = strtok_r(NULL, ":", &save)) {
      TdDirPtr dir = taosOpenDir(path);
      if (dir == NULL) continue;
      TdDirEntryPtr entry;
      while ((entry = taosReadDir(dir)) != NULL) {
        const char *name = taosGetDirEntryName(entry);
        if (name[0] == '.') continue;
        locale = regexTryLocale(name, charset);
        if (locale != (locale_t)0) break;
      }
      (void)taosCloseDir(&dir);
      if (locale != (locale_t)0) break;
    }
    taosMemoryFree(copy);
    if (locale != (locale_t)0) return locale;
  }
  // A fixed command only: no client-supplied text is interpreted by a shell.
#ifdef __linux__
  // Do not inject the caller's instrumentation into the system utility. LSan
  // can terminate locale(1) before its buffered locale list reaches the pipe.
  TdCmdPtr command = taosOpenCmd("/usr/bin/env -u LD_PRELOAD /usr/bin/locale -a");
#else
  TdCmdPtr command = taosOpenCmd("/usr/bin/locale -a");
#endif
  if (command != NULL) {
    char name[256];
    while (taosGetsCmd(command, sizeof(name), name) > 0) {
      if (locale != (locale_t)0) continue;  // Drain the pipe before pclose.
      name[strcspn(name, "\r\n")] = 0;
      locale = regexTryLocale(name, charset);
    }
    taosCloseCmd(&command);
  }
  return locale;
}

// Called with poolLock held and query memory pooling disabled. The cache owns
// the returned locale, which can be duplicated for a matcher pool.
static int32_t regexGetLocaleLocked(const char *charset, locale_t *result) {
  if (sRegexCache.localeHash == NULL) {
    sRegexCache.localeHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (sRegexCache.localeHash == NULL) {
      return terrno;
    }
    taosHashSetFreeFp(sRegexCache.localeHash, regexLocaleFree);
  }

  size_t len = strlen(charset);
  locale_t *cachedLocale = taosHashGet(sRegexCache.localeHash, charset, len);
  if (cachedLocale != NULL) {
    *result = *cachedLocale;
    return TSDB_CODE_SUCCESS;
  }

  locale_t locale = regexFindLocale(charset);
  if (locale == (locale_t)0) {
    uError("no POSIX regex locale for client charset:%s", charset);
    return TSDB_CODE_CHARSET_CONVERT_ERROR;
  }
  int32_t code = taosHashPut(sRegexCache.localeHash, charset, len, &locale, sizeof(locale));
  if (code != TSDB_CODE_SUCCESS) {
    freelocale(locale);
    return code;
  }
  *result = locale;
  return TSDB_CODE_SUCCESS;
}

// poolLock serializes lookup and eviction. Hash references keep a removed pool
// alive until its last borrower/waiter releases it.
static int32_t regexEvictPool(void) {
  if (sRegexCache.poolCount < MAX_REGEX_CACHE_SIZE) return 0;
  char *oldestKey = NULL;
  size_t oldestLen = 0;
  int64_t oldestTime = INT64_MAX;
  SRegexPool **entry = taosHashIterate(sRegexCache.poolHash, NULL);
  while (entry != NULL) {
    int64_t used = atomic_load_64(&(*entry)->lastUsed);
    if (used < oldestTime) {
      size_t len = 0;
      void *key = taosHashGetKey(entry, &len);
      char *copy = taosMemoryRealloc(oldestKey, len);
      if (copy == NULL) {
        taosHashCancelIterate(sRegexCache.poolHash, entry);
        taosMemoryFree(oldestKey);
        return terrno;
      }
      oldestKey = copy;
      memcpy(oldestKey, key, len);
      oldestLen = len;
      oldestTime = used;
    }
    entry = taosHashIterate(sRegexCache.poolHash, entry);
  }
  int32_t code = oldestKey == NULL ? 0 : taosHashRemove(sRegexCache.poolHash, oldestKey, oldestLen);
  if (oldestKey != NULL && code == 0) --sRegexCache.poolCount;
  taosMemoryFree(oldestKey);
  return code;
}

static int32_t regexPoolAcquire(const char *pattern, const char *charset, SRegexPool ***reference) {
  size_t charsetLen = strlen(charset) + 1;
  size_t patternLen = strlen(pattern);
  char *key = taosMemoryMalloc(charsetLen + patternLen);
  if (key == NULL) return terrno;
  memcpy(key, charset, charsetLen);
  memcpy(key + charsetLen, pattern, patternLen);
  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&sRegexCache.poolLock);
  if (sRegexCache.poolHash == NULL) {
    sRegexCache.poolHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (sRegexCache.poolHash == NULL) { code = terrno; goto _done; }
    taosHashSetFreeFp(sRegexCache.poolHash, regexPoolFree);
  }
  *reference = taosHashAcquire(sRegexCache.poolHash, key, charsetLen + patternLen);
  if (*reference == NULL) {
    SRegexPool *pool = taosMemoryCalloc(1, sizeof(*pool));
    if (pool == NULL) { code = terrno; goto _done; }
    code = taosThreadMutexInit(&pool->mutex, NULL);
    if (code != 0) { taosMemoryFree(pool); goto _done; }
    code = taosThreadCondInit(&pool->available, NULL);
    if (code != 0) {
      (void)taosThreadMutexDestroy(&pool->mutex);
      taosMemoryFree(pool);
      goto _done;
    }
    pool->count = TMAX(1, atomic_load_32(&regexPoolSize));
    locale_t locale = (locale_t)0;
    code = regexGetLocaleLocked(charset, &locale);
    if (code == TSDB_CODE_SUCCESS) {
      pool->locale = duplocale(locale);
      if (pool->locale == (locale_t)0) {
        code = TAOS_SYSTEM_ERROR(ERRNO);
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      pool->slots = taosMemoryCalloc(pool->count, sizeof(regex_t));
      pool->busy = taosMemoryCalloc(pool->count, sizeof(bool));
      if (pool->slots == NULL || pool->busy == NULL) code = terrno;
    }
    if (code == TSDB_CODE_SUCCESS) {
      locale_t previous = uselocale(pool->locale);
      if (previous == (locale_t)0) {
        code = TAOS_SYSTEM_ERROR(ERRNO);
        uError("failed to select regex compilation locale for charset:%s, code:%d", charset, code);
      } else {
        for (int32_t i = 0; i < pool->count; ++i) {
          if (regcomp(&pool->slots[i], pattern, REG_EXTENDED) != 0) {
            code = TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
            break;
          }
          ++pool->compiled;
        }
        // No executor callbacks or longjmp-capable code runs in this scope.
        (void)uselocale(previous);
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      pool->lastUsed = taosGetTimestampSec();
      code = regexEvictPool();
      if (code == 0) {
        code = taosHashPut(sRegexCache.poolHash, key, charsetLen + patternLen, &pool, sizeof(pool));
        if (code == 0) ++sRegexCache.poolCount;
      }
    }
    if (code != TSDB_CODE_SUCCESS) {
      regexPoolFree(&pool);
    } else {
      *reference = taosHashAcquire(sRegexCache.poolHash, key, charsetLen + patternLen);
      if (*reference == NULL) code = TSDB_CODE_APP_ERROR;
    }
  }
_done:
  (void)taosThreadMutexUnlock(&sRegexCache.poolLock);
  taosMemoryFree(key);
  return code;
}
#endif

void taosRegexSetPoolSize(int32_t threads) {
#ifndef WINDOWS
  atomic_store_32(&regexPoolSize, TMAX(1, threads));
#endif
}

int32_t taosCheckRegexLocale(void *charsetCxt) {
#ifdef WINDOWS
  (void)charsetCxt;
  return TSDB_CODE_SUCCESS;
#else
  int32_t previousCancel = 0;
  int32_t code = taosThreadSetCancelState(PTHREAD_CANCEL_DISABLE, &previousCancel);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Discovery can read directories or a subprocess pipe, and the cached
  // locale must outlive the query requesting it.
  bool poolEnabled = false;
  bool randomErrors = false;
  (void)poolEnabled;
  (void)randomErrors;
  taosSaveDisableMemPoolUsage(poolEnabled, randomErrors);
  code = taosThreadMutexLock(&sRegexCache.poolLock);
  if (code == TSDB_CODE_SUCCESS) {
    locale_t locale = (locale_t)0;
    code = regexGetLocaleLocked(regexCanonicalCharset(taosConvCharset(charsetCxt)), &locale);
    (void)taosThreadMutexUnlock(&sRegexCache.poolLock);
  }
  taosRestoreEnableMemPoolUsage(poolEnabled, randomErrors);
  (void)taosThreadSetCancelState(previousCancel, NULL);
  return code;
#endif
}

static void checkRegexCache(void *param, void *tmrId) {
  int32_t code = 0;
  taosRLockLatch(&sRegexCache.mutex);
  if (sRegexCache.exit) {
    goto _exit;
  }
  bool stopped = taosTmrReset(checkRegexCache, REGEX_CACHE_CLEAR_TIME * 1000, param, sRegexCache.regexCacheTmr, &tmrId);
  if (stopped) {
    uError("failed to reset regex cache timer");
    goto _exit;
  }
#ifndef WINDOWS
  (void)taosThreadMutexLock(&sRegexCache.poolLock);
  if (sRegexCache.poolHash != NULL && taosHashGetSize(sRegexCache.poolHash) >= MAX_REGEX_CACHE_SIZE) {
    SRegexPool **entry = taosHashIterate(sRegexCache.poolHash, NULL);
    while (entry != NULL) {
      if (taosGetTimestampSec() - atomic_load_64(&(*entry)->lastUsed) > REGEX_CACHE_CLEAR_TIME) {
        size_t len = 0;
        void *key = taosHashGetKey(entry, &len);
        if (taosHashRemove(sRegexCache.poolHash, key, len) == 0) --sRegexCache.poolCount;
      }
      entry = taosHashIterate(sRegexCache.poolHash, entry);
    }
  }
  (void)taosThreadMutexUnlock(&sRegexCache.poolLock);
#endif
  if (taosHashGetSize(sRegexCache.regexHash) < MAX_REGEX_CACHE_SIZE) {
    goto _exit;
  }

  if (taosHashGetSize(sRegexCache.regexHash) >= MAX_REGEX_CACHE_SIZE) {
    UsingRegex **ppUsingRegex = taosHashIterate(sRegexCache.regexHash, NULL);
    while ((ppUsingRegex != NULL)) {
      if (taosGetTimestampSec() - (*ppUsingRegex)->lastUsedTime > REGEX_CACHE_CLEAR_TIME) {
        size_t len = 0;
        char  *key = (char *)taosHashGetKey(ppUsingRegex, &len);
        if (TSDB_CODE_SUCCESS != taosHashRemove(sRegexCache.regexHash, key, len)) {
          uError("failed to remove regex pattern %s from cache", key);
          goto _exit;
        }
      }
      ppUsingRegex = taosHashIterate(sRegexCache.regexHash, ppUsingRegex);
    }
  }
_exit:
  taosRUnLockLatch(&sRegexCache.mutex);
}

void regexCacheFree(void *ppUsingRegex) {
  regfree(&(*(UsingRegex **)ppUsingRegex)->pRegex);
  taosMemoryFree(*(UsingRegex **)ppUsingRegex);
}

int32_t InitRegexCache() {
#ifdef WINDOWS
  return 0;
#endif
  if (atomic_val_compare_exchange_8(&sRegexCache.inited, 0, 1) != 0) return TSDB_CODE_SUCCESS;
  sRegexCache.regexHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (sRegexCache.regexHash == NULL) {
    uError("failed to create RegexCache");
    return terrno;
  }
  taosHashSetFreeFp(sRegexCache.regexHash, regexCacheFree);
  sRegexCache.regexCacheTmr = taosTmrInit(0, 0, 0, "REGEXCACHE");
  if (sRegexCache.regexCacheTmr == NULL) {
    uError("failed to create regex cache check timer");
    return terrno;
  }

  sRegexCache.exit = false;
  taosInitRWLatch(&sRegexCache.mutex);
  sRegexCache.timer = taosTmrStart(checkRegexCache, REGEX_CACHE_CLEAR_TIME * 1000, NULL, sRegexCache.regexCacheTmr);
  if (sRegexCache.timer == NULL) {
    uError("failed to start regex cache timer");
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

void DestroyRegexCache() {
#ifdef WINDOWS
  return;
#endif
  int32_t code = 0;
  uInfo("[regex cache] destroy regex cache");
  bool ret = taosTmrStopA(&sRegexCache.timer);
  if (!ret) {
    uInfo("stop regex cache timer may be failed");
  }
  taosWLockLatch(&sRegexCache.mutex);
  sRegexCache.exit = true;
  taosWUnLockLatch(&sRegexCache.mutex);
  taosHashCleanup(sRegexCache.regexHash);
  sRegexCache.regexHash = NULL;
#ifndef WINDOWS
  taosHashCleanup(sRegexCache.poolHash);
  sRegexCache.poolHash = NULL;
  sRegexCache.poolCount = 0;
  taosHashCleanup(sRegexCache.localeHash);
  sRegexCache.localeHash = NULL;
#endif
  taosTmrCleanUp(sRegexCache.regexCacheTmr);
  sRegexCache.regexCacheTmr = NULL;
  atomic_store_8(&sRegexCache.inited, 0);
}

// Client-side validation may lack the execution server's installed locale.
// Validate ASCII syntax locally; non-ASCII POSIX patterns are checked on receipt.
int32_t checkRegexPatternEx(const char *pPattern, char *errBuf, int32_t errLen, void *charsetCxt) {
  if (errBuf != NULL && errLen > 0) errBuf[0] = 0;
  if (pPattern == NULL) return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
#ifndef WINDOWS
  for (const unsigned char *p = (const unsigned char *)pPattern; *p; ++p) {
    if (*p >= 0x80) return TSDB_CODE_SUCCESS;
  }
  charsetCxt = taosConvInit("ANSI_X3.4-1968");
  if (charsetCxt == NULL) return TSDB_CODE_CHARSET_CONVERT_ERROR;
#endif
  int32_t code = taosCheckRegexCharset(pPattern, charsetCxt);
  if (code != 0 && errBuf != NULL && errLen > 0) {
    (void)snprintf(errBuf, errLen, "%s", tstrerror(code));
  }
  return code;
}

int32_t checkRegexPattern(const char *pPattern, void *charsetCxt) {
  return checkRegexPatternEx(pPattern, NULL, 0, charsetCxt);
}

int32_t getRegComp(const char *pPattern, HashRegexPtr **regexRet) {
  HashRegexPtr *ppUsingRegex = (HashRegexPtr *)taosHashAcquire(sRegexCache.regexHash, pPattern, strlen(pPattern));
  if (ppUsingRegex != NULL) {
    (*ppUsingRegex)->lastUsedTime = taosGetTimestampSec();
    *regexRet = ppUsingRegex;
    return TSDB_CODE_SUCCESS;
  }
  UsingRegex *pUsingRegex = taosMemoryMalloc(sizeof(UsingRegex));
  if (pUsingRegex == NULL) {
    uError("Failed to Malloc when compile regex pattern %s.", pPattern);
    return terrno;
  }
  int32_t cflags = REG_EXTENDED;
  int32_t ret = regcomp(&pUsingRegex->pRegex, pPattern, cflags);
  if (ret != 0) {
    char msgbuf[256] = {0};
    (void)regerror(ret, &pUsingRegex->pRegex, msgbuf, tListLen(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pPattern, msgbuf);
    taosMemoryFree(pUsingRegex);
    return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
  }

  while (true) {
    int code = taosHashPut(sRegexCache.regexHash, pPattern, strlen(pPattern), &pUsingRegex, sizeof(UsingRegex *));
    if (code != 0 && code != TSDB_CODE_DUP_KEY) {
      regexCacheFree(&pUsingRegex);
      uError("Failed to put regex pattern %s into cache, exception internal error.", pPattern);
      return code;
    } else if (code == TSDB_CODE_DUP_KEY) {
      terrno = 0;
    }
    ppUsingRegex = (UsingRegex **)taosHashAcquire(sRegexCache.regexHash, pPattern, strlen(pPattern));
    if (ppUsingRegex) {
      if (*ppUsingRegex != pUsingRegex) {
        regexCacheFree(&pUsingRegex);
      }
      pUsingRegex = (*ppUsingRegex);
      break;
    } else {
      continue;
    }
  }
  pUsingRegex->lastUsedTime = taosGetTimestampSec();
  *regexRet = ppUsingRegex;
  return TSDB_CODE_SUCCESS;
}

void releaseRegComp(UsingRegex **regex) { taosHashRelease(sRegexCache.regexHash, regex); }

static threadlocal UsingRegex **ppUsingRegex;
static threadlocal regex_t     *pRegex;
static threadlocal char        *pOldPattern = NULL;

#ifdef WINDOWS
static threadlocal regex_t gRegex;

void DestoryThreadLocalRegComp() {
  if (NULL != pOldPattern) {
    regfree(&gRegex);
    taosMemoryFree(pOldPattern);
    pOldPattern = NULL;
  }
}

int32_t threadGetRegComp(regex_t **regex, const char *pPattern) {
  if (NULL != pOldPattern) {
    if (strcmp(pOldPattern, pPattern) == 0) {
      *regex = &gRegex;
      return 0;
    } else {
      DestoryThreadLocalRegComp();
    }
  }
  pOldPattern = taosStrdup(pPattern);
  if (NULL == pOldPattern) {
    uError("Failed to Malloc when compile regex pattern %s.", pPattern);
    return terrno;
  }
  int32_t cflags = REG_EXTENDED;
  int32_t ret = regcomp(&gRegex, pPattern, cflags);
  if (ret != 0) {
    char msgbuf[256] = {0};
    (void)regerror(ret, &gRegex, msgbuf, tListLen(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pPattern, msgbuf);
    taosMemoryFree(pOldPattern);
    pOldPattern = NULL;
    return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
  }
  *regex = &gRegex;
  return 0;
}
#else
void DestoryThreadLocalRegComp() {
  if (NULL != pOldPattern) {
    releaseRegComp(ppUsingRegex);
    taosMemoryFree(pOldPattern);
    ppUsingRegex = NULL;
    pRegex = NULL;
    pOldPattern = NULL;
  }
}

int32_t threadGetRegComp(regex_t **regex, const char *pPattern) {
  if (NULL != pOldPattern) {
    if (strcmp(pOldPattern, pPattern) == 0) {
      *regex = pRegex;
      return 0;
    } else {
      DestoryThreadLocalRegComp();
    }
  }

  HashRegexPtr *ppRegex = NULL;
  int32_t       code = getRegComp(pPattern, &ppRegex);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pOldPattern = taosStrdup(pPattern);
  if (NULL == pOldPattern) {
    uError("Failed to Malloc when compile regex pattern %s.", pPattern);
    return terrno;
  }
  ppUsingRegex = ppRegex;
  pRegex = &((*ppUsingRegex)->pRegex);
  *regex = &(*ppRegex)->pRegex;
  return 0;
}
#endif

#if defined(WINDOWS) && defined(USE_PCRE2)
/*
 * The CRT regex engine available on Windows is byte oriented: '.' consumes a
 * single byte, so a pattern such as '^....$' can never match a four character
 * string that is stored as twelve UTF-8 bytes. The POSIX engines used on the
 * other platforms count characters instead. Match with PCRE2 in UTF mode here
 * so that '.' and the quantifiers count code points, while keeping the POSIX
 * path untouched on the platforms that already behave correctly.
 */
typedef struct SWinRegexSlot {
  char       *pattern;
  pcre2_code *utfCode; /* compiled with PCRE2_UTF, used for text payloads */
} SWinRegexSlot;

static threadlocal SWinRegexSlot gWinRegexSlot = {0};

static void winRegexSlotReset(void) {
  if (gWinRegexSlot.utfCode != NULL) {
    pcre2_code_free(gWinRegexSlot.utfCode);
  }
  taosMemoryFree(gWinRegexSlot.pattern);
  gWinRegexSlot.pattern = NULL;
  gWinRegexSlot.utfCode = NULL;
}

static pcre2_code *winRegexCompile(const char *pPattern) {
  if (gWinRegexSlot.pattern == NULL || strcmp(gWinRegexSlot.pattern, pPattern) != 0) {
    winRegexSlotReset();
    gWinRegexSlot.pattern = taosStrdup(pPattern);
    if (gWinRegexSlot.pattern == NULL) {
      return NULL;
    }
  }

  pcre2_code **slot = &gWinRegexSlot.utfCode;
  if (*slot != NULL) {
    return *slot;
  }

  int32_t    errorCode = 0;
  PCRE2_SIZE errorOffset = 0;
  *slot = pcre2_compile((PCRE2_SPTR)pPattern, PCRE2_ZERO_TERMINATED, PCRE2_UTF, &errorCode, &errorOffset,
                        NULL);
  if (*slot == NULL) {
    PCRE2_UCHAR msg[256] = {0};
    (void)pcre2_get_error_message(errorCode, msg, sizeof(msg));
    uDebug("failed to compile regex pattern %s, reason:%s", pPattern, (char *)msg);
  }
  return *slot;
}

#endif

#ifdef WINDOWS
static int32_t winRegexCharsetImpl(const char *string, const char *pattern, size_t count,
                               regmatch_t *matches, int32_t flags, void *charsetCxt) {
#ifndef USE_PCRE2
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  if (string == NULL || pattern == NULL || (count > 0 && matches == NULL)) return TSDB_CODE_INVALID_PARA;
  void *utf8 = taosConvInit("UTF-8");
  if (utf8 == NULL) return TSDB_CODE_CHARSET_CONVERT_ERROR;
  char *subject = NULL;
  char *expression = NULL;
  pcre2_match_data *data = NULL;
  int32_t result = taosMbsToMbs(string, strlen(string), charsetCxt, utf8, &subject, NULL);
  if (result != 0) goto _exit;
  result = taosMbsToMbs(pattern, strlen(pattern), charsetCxt, utf8, &expression, NULL);
  if (result != 0) goto _exit;
  pcre2_code *compiled = winRegexCompile(expression);
  if (compiled == NULL) { result = TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR; goto _exit; }
  data = pcre2_match_data_create_from_pattern(compiled, NULL);
  if (data == NULL) { result = TSDB_CODE_OUT_OF_MEMORY; goto _exit; }
  result = pcre2_match(compiled, (PCRE2_SPTR)subject, PCRE2_ZERO_TERMINATED, 0,
                       flags & REG_NOTBOL ? PCRE2_NOTBOL : 0, data, NULL);
  if (result == PCRE2_ERROR_NOMATCH) { result = 1; goto _exit; }
  if (result < 0) { result = TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR; goto _exit; }
  result = 0;
  PCRE2_SIZE *offsets = pcre2_get_ovector_pointer(data);
  uint32_t groups = pcre2_get_ovector_count(data);
  for (size_t i = 0; i < count; ++i) {
    matches[i].rm_so = matches[i].rm_eo = -1;
    if (i >= groups || offsets[2 * i] == PCRE2_UNSET) continue;
    // Return byte offsets in the caller's encoding, not PCRE2's UTF-8 buffer.
    for (int32_t end = 0; end < 2; ++end) {
      int32_t bytes = 0;
      size_t characters = 0;
      for (size_t j = 0; j < offsets[2 * i + end]; ++j) {
        if (((unsigned char)subject[j] & 0xc0) != 0x80) ++characters;
      }
      result = taosMbsCharOffset(string, strlen(string), characters, charsetCxt, &bytes);
      if (result != 0) goto _exit;
      if (end == 0) matches[i].rm_so = bytes;
      else matches[i].rm_eo = bytes;
    }
  }
_exit:
  if (data != NULL) pcre2_match_data_free(data);
  taosMemoryFree(subject);
  taosMemoryFree(expression);
  return result;
#endif
}
#endif

int32_t taosExecRegexCapturesCharset(const char *string, const char *pattern, size_t count,
                                    regmatch_t *matches, int32_t flags, void *charsetCxt) {
#ifdef WINDOWS
  // Cached objects outlive the query whose memory pool happens to be active.
  bool poolEnabled = false;
  bool randomErrors = false;
  (void)poolEnabled;
  (void)randomErrors;
  taosSaveDisableMemPoolUsage(poolEnabled, randomErrors);
  int32_t code = winRegexCharsetImpl(string, pattern, count, matches, flags, charsetCxt);
  taosRestoreEnableMemPoolUsage(poolEnabled, randomErrors);
  return code;
#else
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t previousCancel = 0;
  if (pattern == NULL || string == NULL) return TSDB_CODE_INVALID_PARA;
  if (count > 0 && matches == NULL) return TSDB_CODE_INVALID_PARA;
  // BSD regcomp rejects an empty ERE; replacement still needs a zero-width
  // match at the current position, including subsequent REG_NOTBOL calls.
  if (pattern[0] == 0) {
    for (size_t i = 0; i < count; ++i) {
      matches[i].rm_so = matches[i].rm_eo = i == 0 ? 0 : -1;
    }
    return TSDB_CODE_SUCCESS;
  }
  // Query kill is cooperative: executor longjmps only after this function
  // returns. Defer pthread cancellation too, until locale, slot and hash
  // reference have all been restored. No callbacks run while locale is changed.
  code = taosThreadSetCancelState(PTHREAD_CANCEL_DISABLE, &previousCancel);
  if (code != 0) return code;
  // Cached objects outlive the query whose memory pool happens to be active.
  bool poolEnabled = false;
  bool randomErrors = false;
  (void)poolEnabled;
  (void)randomErrors;
  taosSaveDisableMemPoolUsage(poolEnabled, randomErrors);
  SRegexPool **reference = NULL;
  code = regexPoolAcquire(pattern, regexCanonicalCharset(taosConvCharset(charsetCxt)), &reference);
  if (code == TSDB_CODE_SUCCESS) {
    SRegexPool *pool = *reference;
    int32_t slot = -1;
    (void)taosThreadMutexLock(&pool->mutex);
    while (slot < 0) {
      for (int32_t i = 0; i < pool->count; ++i) {
        if (!pool->busy[i]) { slot = i; pool->busy[i] = true; break; }
      }
      if (slot < 0) {
        code = taosThreadCondWait(&pool->available, &pool->mutex);
        if (code != 0) break;
      }
    }
    (void)taosThreadMutexUnlock(&pool->mutex);
    if (slot >= 0) {
      if (code == TSDB_CODE_SUCCESS) {
        locale_t previous = uselocale(pool->locale);
        if (previous == (locale_t)0) {
          code = TAOS_SYSTEM_ERROR(ERRNO);
          uError("failed to select regex matching locale, code:%d", code);
        } else {
          int32_t result = regexec(&pool->slots[slot], string, count, matches, flags);
          (void)uselocale(previous);
          if (result == REG_NOMATCH) code = 1;
          else if (result != 0) code = TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;

        }
      }
      (void)taosThreadMutexLock(&pool->mutex);
      pool->busy[slot] = false;
      (void)taosThreadCondSignal(&pool->available);
      (void)taosThreadMutexUnlock(&pool->mutex);
    }
    atomic_store_64(&pool->lastUsed, taosGetTimestampSec());
    taosHashRelease(sRegexCache.poolHash, reference);
  }
  // A pending cancellation may take effect here, after all scoped state is gone.
  taosRestoreEnableMemPoolUsage(poolEnabled, randomErrors);
  (void)taosThreadSetCancelState(previousCancel, NULL);
  return code;
#endif
}

int32_t taosExecRegexGroupsCharset(const char *string, const char *pattern, int32_t group,
                                  int32_t *start, int32_t *len, void *charsetCxt) {
  if (string == NULL || pattern == NULL) return TSDB_CODE_INVALID_PARA;
  // MATCH and extraction retain their historical empty-pattern behavior.
  // The capture API also serves tag replacement, where an empty ERE matches.
  if (group < 0) {
    if (*pattern == 0) return 1;
    return taosExecRegexCapturesCharset(string, pattern, 0, NULL, 0, charsetCxt);
  }
  if (start == NULL || len == NULL) return TSDB_CODE_INVALID_PARA;
  *start = -1;
  *len = 0;
  if (*pattern == 0) return 1;
  size_t count = (size_t)group + 1;
  regmatch_t local[16];
  regmatch_t *matches = count <= tListLen(local) ? local : taosMemoryCalloc(count, sizeof(*matches));
  if (matches == NULL) return terrno;
  int32_t code = taosExecRegexCapturesCharset(string, pattern, count, matches, 0, charsetCxt);
  if (code == 0 && matches[group].rm_so >= 0) {
    *start = matches[group].rm_so;
    *len = matches[group].rm_eo - matches[group].rm_so;
  }
  if (matches != local) taosMemoryFree(matches);
  return code;
}

int32_t taosExecRegexMatchCharset(const char *string, const char *pattern, void *charsetCxt) {
  return taosExecRegexGroupsCharset(string, pattern, -1, NULL, NULL, charsetCxt);
}

int32_t taosCheckRegexCharset(const char *pattern, void *charsetCxt) {
  // Compile through the same pool and encoding that execution will use.
  int32_t code = taosExecRegexMatchCharset("", pattern, charsetCxt);
  return code == 1 ? TSDB_CODE_SUCCESS : code;
}

static int32_t regexValueToString(const void *value, int32_t type, void *charsetCxt, char **out) {
  if (!IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_JSON) return TSDB_CODE_QRY_INVALID_INPUT;
  int32_t len = IS_STR_DATA_BLOB(type) ? blobDataLen(value) : varDataLen(value);
  const char *data = IS_STR_DATA_BLOB(type) ? blobDataVal(value) : varDataVal(value);
  *out = taosMemoryMalloc((size_t)len + 1);
  if (*out == NULL) return terrno;
  if (type == TSDB_DATA_TYPE_NCHAR) {
    len = taosUcs4ToMbs((TdUcs4 *)data, len, *out, charsetCxt);
    if (len < 0) { taosMemoryFreeClear(*out); return TSDB_CODE_SCALAR_CONVERT_NCHAR_ERROR; }
  } else {
    memcpy(*out, data, len);
  }
  (*out)[len] = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t taosCheckRegexValue(const void *value, int32_t type, void *charsetCxt) {
  char *pattern = NULL;
  int32_t code = regexValueToString(value, type, charsetCxt, &pattern);
  if (code == 0) code = taosCheckRegexCharset(pattern, charsetCxt);
  taosMemoryFree(pattern);
  return code;
}

int32_t taosCompareRegexCharset(const void *left, int32_t leftType, const void *right, int32_t rightType,
                               void *charsetCxt, int32_t *result) {
  char *string = NULL;
  char *pattern = NULL;
  int32_t code = regexValueToString(left, leftType, charsetCxt, &string);
  if (code == TSDB_CODE_SUCCESS) code = regexValueToString(right, rightType, charsetCxt, &pattern);
  if (code == TSDB_CODE_SUCCESS) {
    int32_t ret = taosExecRegexMatchCharset(string, pattern, charsetCxt);
    if (ret == 0 || ret == 1) *result = ret;
    else code = ret;
  }
  taosMemoryFree(string);
  taosMemoryFree(pattern);
  return code;
}

int32_t taosArrayCompareString(const void *a, const void *b) {
  const char *x = *(const char **)a;
  const char *y = *(const char **)b;

  return strcmp(x, y);
}

int32_t comparestrPatternMatch(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  if (varDataTLen(pRight) > TSDB_MAX_FIELD_LEN) {
    return 1;
  }
  size_t pLen = varDataLen(pRight);
  size_t sz = varDataLen(pLeft);

  int32_t ret = patternMatch(varDataVal(pRight), pLen, varDataVal(pLeft), sz, &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t comparestrPatternNMatch(const void *pLeft, const void *pRight) {
  return comparestrPatternMatch(pLeft, pRight) ? 0 : 1;
}

int32_t compareBlobPatternMatch(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  if (blobDataTLen(pRight) > TSDB_MAX_BLOB_LEN) {
    return 1;
  }

  int32_t ret =
      patternMatch(blobDataVal(pRight), blobDataLen(pRight), blobDataVal(pLeft), blobDataLen(pLeft), &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t compareBlobPatternNMatch(const void *pLeft, const void *pRight) {
  return compareBlobPatternMatch(pLeft, pRight) ? 0 : 1;
}

int32_t comparewcsPatternMatch(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  size_t psize = varDataLen(pRight);

  int32_t ret = wcsPatternMatch((TdUcs4 *)varDataVal(pRight), psize / TSDB_NCHAR_SIZE, (TdUcs4 *)varDataVal(pLeft),
                                varDataLen(pLeft) / TSDB_NCHAR_SIZE, &pInfo);
  return (ret == TSDB_PATTERN_MATCH) ? 0 : 1;
}

int32_t comparewcsPatternNMatch(const void *pLeft, const void *pRight) {
  return comparewcsPatternMatch(pLeft, pRight) ? 0 : 1;
}

__compar_fn_t getComparFunc(int32_t type, int32_t optr) {
  __compar_fn_t comparFn = NULL;

  if (optr == OP_TYPE_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_VARBINARY &&
                             type != TSDB_DATA_TYPE_NCHAR && type != TSDB_DATA_TYPE_GEOMETRY)) {
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
        uError("getComparFunc data type unexpected type:%d, optr:%d", type, optr);
        terrno = TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
        return NULL;
    }
  }

  if (optr == OP_TYPE_NOT_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_VARBINARY &&
                                 type != TSDB_DATA_TYPE_NCHAR && type != TSDB_DATA_TYPE_GEOMETRY)) {
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
        uError("getComparFunc data type unexpected type:%d, optr:%d", type, optr);
        terrno = TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
        return NULL;
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
    case TSDB_DATA_TYPE_VARBINARY:
      if (optr == OP_TYPE_IN) {
        comparFn = compareChkInString;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = compareChkNotInString;
      } else { /* normal relational comparFn */
        comparFn = compareLenBinaryVal;
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = NULL;  // Regex callers use taosCompareRegexCharset.
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = NULL;  // Regex callers use taosCompareRegexCharset.
      } else if (optr == OP_TYPE_LIKE) { /* wildcard query using like operator */
        comparFn = comparestrPatternMatch;
      } else if (optr == OP_TYPE_NOT_LIKE) { /* wildcard query using like operator */
        comparFn = comparestrPatternNMatch;
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
        comparFn = NULL;  // Regex callers use taosCompareRegexCharset.
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = NULL;  // Regex callers use taosCompareRegexCharset.
      } else if (optr == OP_TYPE_LIKE) {
        comparFn = comparewcsPatternMatch;
      } else if (optr == OP_TYPE_NOT_LIKE) {
        comparFn = comparewcsPatternNMatch;
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

// Sorting and window peers require a transitive order, including at zero.
static int32_t compareFloatingKeys(double left, double right) {
  if (isnan(left)) return isnan(right) ? 0 : -1;
  if (isnan(right)) return 1;
  return (left > right) - (left < right);
}

static int32_t compareFloatKey(const void *left, const void *right) {
  return compareFloatingKeys(GET_FLOAT_VAL(left), GET_FLOAT_VAL(right));
}

static int32_t compareDoubleKey(const void *left, const void *right) {
  return compareFloatingKeys(GET_DOUBLE_VAL(left), GET_DOUBLE_VAL(right));
}

static int32_t compareFloatKeyDesc(const void *left, const void *right) { return compareFloatKey(right, left); }
static int32_t compareDoubleKeyDesc(const void *left, const void *right) { return compareDoubleKey(right, left); }

static int32_t compareNcharSortKey(const void *left, const void *right) {
  int32_t leftLen = varDataLen(left), rightLen = varDataLen(right);
  int32_t common = TMIN(leftLen, rightLen);
  for (int32_t offset = 0; offset + TSDB_NCHAR_SIZE <= common; offset += TSDB_NCHAR_SIZE) {
    uint32_t a, b;
    memcpy(&a, varDataVal(left) + offset, sizeof(a));
    memcpy(&b, varDataVal(right) + offset, sizeof(b));
    if (a != b) return a < b ? -1 : 1;
  }
  return (leftLen > rightLen) - (leftLen < rightLen);
}

static int32_t compareNcharSortKeyDesc(const void *left, const void *right) { return compareNcharSortKey(right, left); }

static int32_t compareJsonSortKey(const void *left, const void *right) {
  const char *a = left, *b = right;
  if (*a == *b) {
    if (*a == TSDB_DATA_TYPE_DOUBLE) {
      double x, y;
      memcpy(&x, a + 1, sizeof(x));
      memcpy(&y, b + 1, sizeof(y));
      return compareFloatingKeys(x, y);
    }
    if (*a == TSDB_DATA_TYPE_NCHAR) return compareNcharSortKey(a + 1, b + 1);
  }
  return compareJsonVal(left, right);
}

static int32_t compareJsonSortKeyDesc(const void *left, const void *right) { return compareJsonSortKey(right, left); }

// OVER sorts compare the same complete keys as partition boundaries and peer groups.
__compar_fn_t getWindowKeyComparFunc(int32_t keyType, int32_t order) {
  switch (keyType) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      return order == TSDB_ORDER_ASC ? compareLenBinaryVal : compareLenBinaryValDesc;
    case TSDB_DATA_TYPE_NCHAR:
      return order == TSDB_ORDER_ASC ? compareNcharSortKey : compareNcharSortKeyDesc;
    case TSDB_DATA_TYPE_FLOAT:
      return order == TSDB_ORDER_ASC ? compareFloatKey : compareFloatKeyDesc;
    case TSDB_DATA_TYPE_DOUBLE:
      return order == TSDB_ORDER_ASC ? compareDoubleKey : compareDoubleKeyDesc;
    case TSDB_DATA_TYPE_JSON:
      return order == TSDB_ORDER_ASC ? compareJsonSortKey : compareJsonSortKeyDesc;
    default:
      return getKeyComparFunc(keyType, order);
  }
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
    case TSDB_DATA_TYPE_VARBINARY:
      return (order == TSDB_ORDER_ASC) ? compareLenBinaryVal : compareLenBinaryValDesc;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      return (order == TSDB_ORDER_ASC) ? compareLenPrefixedStr : compareLenPrefixedStrDesc;
    case TSDB_DATA_TYPE_NCHAR:
      return (order == TSDB_ORDER_ASC) ? compareLenPrefixedWStr : compareLenPrefixedWStrDesc;
    case TSDB_DATA_TYPE_JSON:
      return (order == TSDB_ORDER_ASC) ? compareJsonVal : compareJsonValDesc;
    case TSDB_DATA_TYPE_DECIMAL64:
      return (order == TSDB_ORDER_ASC) ? compareDecimal64SameScale : compareDecimal64SameScaleDesc;
    case TSDB_DATA_TYPE_DECIMAL:
      return (order == TSDB_ORDER_ASC) ? compareDecimal128SameScale : compareDecimal128SameScaleDesc;
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      return (order == TSDB_ORDER_ASC) ? compareBlobVal : compareBlobValDesc;
    default:
      return (order == TSDB_ORDER_ASC) ? compareInt32Val : compareInt32ValDesc;
  }
}
