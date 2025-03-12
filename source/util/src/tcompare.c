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
#include "regex.h"
#include "tdef.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"
#include "types.h"
#include "osString.h"
#include "ttimer.h"

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

int32_t compareChkInString(const void *pLeft, const void *pRight) {
  return NULL != taosHashGet((SHashObj *)pRight, pLeft, varDataTLen(pLeft)) ? 1 : 0;
}

int32_t compareChkNotInString(const void *pLeft, const void *pRight) {
  return NULL == taosHashGet((SHashObj *)pRight, pLeft, varDataTLen(pLeft)) ? 1 : 0;
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
  int64_t left = GET_INT64_VAL(pLeft), right = GET_INT64_VAL(pRight);
  if (left > right) return 1;
  if (left < right) return -1;
  return 0;
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

  if (FLT_EQUAL(p1, p2)) {
    return 0;
  }
  return FLT_GREATER(p1, p2) ? 1 : -1;
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

  int32_t ret = tasoUcs4Compare((TdUcs4 *)varDataVal(pLeft), (TdUcs4 *)varDataVal(pRight), len1>len2 ? len2:len1);
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

int32_t compareLenBinaryValDesc(const void *pLeft, const void *pRight) {
  return compareLenBinaryVal(pRight, pLeft);
}

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
  int8_t   left = GET_INT8_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt8Uint64(const void *pLeft, const void *pRight) {
  int8_t   left = GET_INT8_VAL(pLeft);
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
  int16_t  left = GET_INT16_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt16Uint64(const void *pLeft, const void *pRight) {
  int16_t  left = GET_INT16_VAL(pLeft);
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
  int32_t  left = GET_INT32_VAL(pLeft);
  if (left < 0) return -1;
  uint32_t right = GET_UINT32_VAL(pRight);
  if ((uint32_t)left > right) return 1;
  if ((uint32_t)left < right) return -1;
  return 0;
}

int32_t compareInt32Uint64(const void *pLeft, const void *pRight) {
  int32_t  left = GET_INT32_VAL(pLeft);
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
  int64_t  left = GET_INT64_VAL(pLeft);
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
    if (c == pInfo->matchAll) { /* Match "*" */

      while ((i < psize) && ((c = pattern[i++]) == pInfo->matchAll || c == pInfo->matchOne)) {
        if (c == pInfo->matchOne) {
          if (j >= ssize || str[j++] == 0) {  // empty string, return not match
            return TSDB_PATTERN_NOWILDCARDMATCH;
          } else {
            ++nMatchChar;
          }
        }
      }

      if (i >= psize && (c == pInfo->umatchOne || c == pInfo->umatchAll)) {
        return TSDB_PATTERN_MATCH; /* "*" at the end of the pattern matches */
      }

      if (c == '\\' && (pattern[i] == '_' || pattern[i] == '%')) {
        c = pattern[i];
        i++;
      }

      char rejectList[2] = {toupper(c), tolower(c)};

      str += nMatchChar;
      int32_t remain = ssize - nMatchChar;
      while (1) {
        size_t n = tstrncspn(str, remain, rejectList, 2);

        str += n;
        remain -= n;

        if ((remain <= 0) || str[0] == 0) {
          break;
        }

        int32_t ret = patternMatch(&pattern[i], psize - i, ++str, --remain, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    if (j < ssize) {
      c1 = str[j++];
      ++nMatchChar;

      if (c == '\\' && (pattern[i] == '_' || pattern[i] == '%')) {
        if (c1 != pattern[i]) {
          return TSDB_PATTERN_NOMATCH;
        } else {
          i++;
          continue;
        }
      }

      if (c == c1 || tolower(c) == tolower(c1) || (c == pInfo->matchOne && c1 != 0)) {
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

int32_t wcsPatternMatch(const TdUcs4 *pattern, size_t psize, const TdUcs4 *str, size_t ssize,
                        const SPatternCompareInfo *pInfo) {
  TdUcs4 c, c1;

  int32_t i = 0;
  int32_t j = 0;
  int32_t nMatchChar = 0;

  while ((i < psize) && ((c = pattern[i++]) != 0)) {
    if (c == pInfo->umatchAll) { /* Match "%" */

      while ((i < psize) && ((c = pattern[i++]) == pInfo->umatchAll || c == pInfo->umatchOne)) {
        if (c == pInfo->umatchOne) {
          if (j >= ssize || str[j++] == 0) {
            return TSDB_PATTERN_NOWILDCARDMATCH;
          } else {
            ++nMatchChar;
          }
        }
      }

      if (i >= psize && (c == pInfo->umatchOne || c == pInfo->umatchAll)) {
        return TSDB_PATTERN_MATCH;
      }

      if (c == '\\' && (pattern[i] == '_' || pattern[i] == '%')) {
        c = pattern[i];
        i++;
      }

      TdUcs4 rejectList[2] = {towupper(c), towlower(c)};

      str += nMatchChar;
      int32_t remain = ssize - nMatchChar;
      while (1) {
        size_t n = twcsncspn(str, remain, rejectList, 2);

        str += n;
        remain -= n;

        if ((remain <= 0) || str[0] == 0) {
          break;
        }

        int32_t ret = wcsPatternMatch(&pattern[i], psize - i, ++str, --remain, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    if (j < ssize) {
      c1 = str[j++];
      nMatchChar++;

      if (c == '\\' && (pattern[i] == '_' || pattern[i] == '%')) {
        if (c1 != pattern[i]) {
          return TSDB_PATTERN_NOMATCH;
        } else {
          i++;
          continue;
        }
      }

      if (c == c1 || towlower(c) == towlower(c1) || (c == pInfo->umatchOne && c1 != 0)) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (j >= ssize || str[j] == 0) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int32_t comparestrRegexNMatch(const void *pLeft, const void *pRight) {
  return comparestrRegexMatch(pLeft, pRight) ? 0 : 1;
}

typedef struct UsingRegex {
  regex_t pRegex;
  int32_t lastUsedTime;
} UsingRegex;
typedef UsingRegex* HashRegexPtr;

typedef struct RegexCache {
  SHashObj      *regexHash;
  void          *regexCacheTmr;
  void          *timer;
  SRWLatch      mutex;
  bool          exit;
} RegexCache;
static RegexCache sRegexCache;
#define MAX_REGEX_CACHE_SIZE   20
#define REGEX_CACHE_CLEAR_TIME 30

static void checkRegexCache(void* param, void* tmrId) {
  int32_t  code = 0;
  taosRLockLatch(&sRegexCache.mutex);
  if(sRegexCache.exit) {
    goto _exit;
  }
  bool stopped = taosTmrReset(checkRegexCache, REGEX_CACHE_CLEAR_TIME * 1000, param, sRegexCache.regexCacheTmr, &tmrId);
  if (stopped) {
    uError("failed to reset regex cache timer");
    goto _exit;
  }
  if (taosHashGetSize(sRegexCache.regexHash) < MAX_REGEX_CACHE_SIZE) {
    goto _exit;
  }

  if (taosHashGetSize(sRegexCache.regexHash) >= MAX_REGEX_CACHE_SIZE) {
    UsingRegex **ppUsingRegex = taosHashIterate(sRegexCache.regexHash, NULL);
    while ((ppUsingRegex != NULL)) {
      if (taosGetTimestampSec() - (*ppUsingRegex)->lastUsedTime > REGEX_CACHE_CLEAR_TIME) {
        size_t len = 0;
        char* key = (char*)taosHashGetKey(ppUsingRegex, &len);
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

void DestroyRegexCache(){
  #ifdef WINDOWS
    return;
  #endif
  int32_t code = 0;
  uInfo("[regex cache] destory regex cache");
  bool ret = taosTmrStopA(&sRegexCache.timer);
  if (!ret) {
    uInfo("stop regex cache timer may be failed");
  }
  taosWLockLatch(&sRegexCache.mutex);
  sRegexCache.exit = true;
  taosHashCleanup(sRegexCache.regexHash);
  taosTmrCleanUp(sRegexCache.regexCacheTmr);
  taosWUnLockLatch(&sRegexCache.mutex);
}

int32_t checkRegexPattern(const char *pPattern) {
  if (pPattern == NULL) {
    return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
  }

  regex_t regex;
  int32_t cflags = REG_EXTENDED;
  int32_t ret = regcomp(&regex, pPattern, cflags);
  if (ret != 0) {
    char msgbuf[256] = {0};
    (void)regerror(ret, &regex, msgbuf, tListLen(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pPattern, msgbuf);
    return TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR;
  }
  regfree(&regex);
  return TSDB_CODE_SUCCESS;
}

int32_t getRegComp(const char *pPattern, HashRegexPtr **regexRet) {
  HashRegexPtr* ppUsingRegex = (HashRegexPtr*)taosHashAcquire(sRegexCache.regexHash, pPattern, strlen(pPattern));
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

void releaseRegComp(UsingRegex  **regex){
  taosHashRelease(sRegexCache.regexHash, regex);
}

static threadlocal UsingRegex ** ppUsingRegex;
static threadlocal regex_t * pRegex;
static threadlocal char    *pOldPattern = NULL;

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
  int32_t code = getRegComp(pPattern, &ppRegex);
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

static int32_t doExecRegexMatch(const char *pString, const char *pPattern) {
  int32_t ret = 0;
  char    msgbuf[256] = {0};

  regex_t *regex = NULL;
  ret = threadGetRegComp(&regex, pPattern);
  if (ret != 0) {
    return ret;
  }

  regmatch_t pmatch[1];
  ret = regexec(regex, pString, 1, pmatch, 0);
  if (ret != 0 && ret != REG_NOMATCH) {
    terrno =  TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR; 
    (void)regerror(ret, regex, msgbuf, sizeof(msgbuf));
    uDebug("Failed to match %s with pattern %s, reason %s", pString, pPattern, msgbuf)
  }

  return (ret == 0) ? 0 : 1;
}

int32_t comparestrRegexMatch(const void *pLeft, const void *pRight) {
  size_t sz = varDataLen(pRight);
  char  *pattern = taosMemoryMalloc(sz + 1);
  if (NULL == pattern) {
    return 1;  // terrno has been set
  }

  (void)memcpy(pattern, varDataVal(pRight), varDataLen(pRight));
  pattern[sz] = 0;

  sz = varDataLen(pLeft);
  char *str = taosMemoryMalloc(sz + 1);
  if (NULL == str) {
    taosMemoryFree(pattern);
    return 1;  // terrno has been set
  }

  (void)memcpy(str, varDataVal(pLeft), sz);
  str[sz] = 0;

  int32_t ret = doExecRegexMatch(str, pattern);

  taosMemoryFree(str);
  taosMemoryFree(pattern);

  return (ret == 0) ? 0 : 1;
}

int32_t comparewcsRegexMatch(const void *pString, const void *pPattern) {
  size_t len = varDataLen(pPattern);
  char  *pattern = taosMemoryMalloc(len + 1);
  if (NULL == pattern) {
    return 1;  // terrno has been set
  }

  int convertLen = taosUcs4ToMbs((TdUcs4 *)varDataVal(pPattern), len, pattern, NULL);
  if (convertLen < 0) {
    taosMemoryFree(pattern);
    return 1; // terrno has been set
  }

  pattern[convertLen] = 0;

  len = varDataLen(pString);
  char *str = taosMemoryMalloc(len + 1);
  if (NULL == str) {
    taosMemoryFree(pattern);
    return 1; // terrno has been set
  }

  convertLen = taosUcs4ToMbs((TdUcs4 *)varDataVal(pString), len, str, NULL);
  if (convertLen < 0) {
    taosMemoryFree(str);
    taosMemoryFree(pattern);
    return 1; // terrno has been set
  }

  str[convertLen] = 0;

  int32_t ret = doExecRegexMatch(str, pattern);

  taosMemoryFree(str);
  taosMemoryFree(pattern);

  return (ret == 0) ? 0 : 1;
}

int32_t comparewcsRegexNMatch(const void *pLeft, const void *pRight) {
  return comparewcsRegexMatch(pLeft, pRight) ? 0 : 1;
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
        comparFn = comparestrRegexMatch;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = comparestrRegexNMatch;
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
        comparFn = comparewcsRegexMatch;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = comparewcsRegexNMatch;
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
    default:
      return (order == TSDB_ORDER_ASC) ? compareInt32Val : compareInt32ValDesc;
  }
}
