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

#include "tcompare.h"


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
      int32_t ret = memcmp((wchar_t*) t1, (wchar_t*) t2, t2->len);
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
