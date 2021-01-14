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

#include "qArithmeticOperator.h"
#include "taosdef.h"
#include "tutil.h"

#define ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, op, _res_type, _ord)     \
  {                                                                                                   \
    int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 \
    int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               \
                                                                                                      \
    if ((len1) == (len2)) {                                                                           \
      for (; i < (len2) && i >= 0; i += step, (out) += 1) {                                           \
        if (isNull((char *)&((left)[i]), _left_type) || isNull((char *)&((right)[i]), _right_type)) { \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] op(right)[i];                                                      \
      }                                                                                               \
    } else if ((len1) == 1) {                                                                         \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                           \
        if (isNull((char *)(left), _left_type) || isNull((char *)&(right)[i], _right_type)) {         \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[0] op(right)[i];                                                      \
      }                                                                                               \
    } else if ((len2) == 1) {                                                                         \
      for (; i >= 0 && i < (len1); i += step, (out) += 1) {                                           \
        if (isNull((char *)&(left)[i], _left_type) || isNull((char *)(right), _right_type)) {         \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] op(right)[0];                                                      \
      }                                                                                               \
    }                                                                                                 \
  }

#define ARRAY_LIST_OP_REM(left, right, _left_type, _right_type, len1, len2, out, op, _res_type, _ord) \
  {                                                                                                   \
    int32_t i = (_ord == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                   \
    int32_t step = (_ord == TSDB_ORDER_ASC) ? 1 : -1;                                                 \
                                                                                                      \
    if (len1 == (len2)) {                                                                             \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                           \
        if (isNull((char *)&(left[i]), _left_type) || isNull((char *)&(right[i]), _right_type)) {     \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] - ((int64_t)(((double)(left)[i]) / (right)[i])) * (right)[i];      \
      }                                                                                               \
    } else if (len1 == 1) {                                                                           \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                           \
        if (isNull((char *)(left), _left_type) || isNull((char *)&((right)[i]), _right_type)) {       \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[0] - ((int64_t)(((double)(left)[0]) / (right)[i])) * (right)[i];      \
      }                                                                                               \
    } else if ((len2) == 1) {                                                                         \
      for (; i >= 0 && i < len1; i += step, (out) += 1) {                                             \
        if (isNull((char *)&((left)[i]), _left_type) || isNull((char *)(right), _right_type)) {       \
          SET_DOUBLE_NULL(out);                                                                       \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] - ((int64_t)(((double)(left)[i]) / (right)[0])) * (right)[0];      \
      }                                                                                               \
    }                                                                                                 \
  }

#define ARRAY_LIST_ADD(left, right, _left_type, _right_type, len1, len2, out, _ord) \
  ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, +, TSDB_DATA_TYPE_DOUBLE, _ord)
#define ARRAY_LIST_SUB(left, right, _left_type, _right_type, len1, len2, out, _ord) \
  ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, -, TSDB_DATA_TYPE_DOUBLE, _ord)
#define ARRAY_LIST_MULTI(left, right, _left_type, _right_type, len1, len2, out, _ord) \
  ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, *, TSDB_DATA_TYPE_DOUBLE, _ord)
#define ARRAY_LIST_DIV(left, right, _left_type, _right_type, len1, len2, out, _ord) \
  ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, /, TSDB_DATA_TYPE_DOUBLE, _ord)
#define ARRAY_LIST_REM(left, right, _left_type, _right_type, len1, len2, out, _ord) \
  ARRAY_LIST_OP_REM(left, right, _left_type, _right_type, len1, len2, out, %, TSDB_DATA_TYPE_DOUBLE, _ord)

#define TYPE_CONVERT_DOUBLE_RES(left, right, out, _type_left, _type_right, _type_res) \
  _type_left * pLeft = (_type_left *)(left);                                          \
  _type_right *pRight = (_type_right *)(right);                                       \
  _type_res *  pOutput = (_type_res *)(out);

#define DO_VECTOR_ADD(left, numLeft, leftType, leftOriginType, right, numRight, rightType, rightOriginType, _output, \
                      _order)                                                                                        \
  do {                                                                                                               \
    TYPE_CONVERT_DOUBLE_RES(left, right, _output, leftOriginType, rightOriginType, double);                          \
    ARRAY_LIST_ADD(pLeft, pRight, leftType, rightType, numLeft, numRight, pOutput, _order);                          \
  } while (0)

#define DO_VECTOR_SUB(left, numLeft, leftType, leftOriginType, right, numRight, rightType, rightOriginType, _output, \
                      _order)                                                                                        \
  do {                                                                                                               \
    TYPE_CONVERT_DOUBLE_RES(left, right, _output, leftOriginType, rightOriginType, double);                          \
    ARRAY_LIST_SUB(pLeft, pRight, leftType, rightType, numLeft, numRight, pOutput, _order);                          \
  } while (0)

#define DO_VECTOR_MULTIPLY(left, numLeft, leftType, leftOriginType, right, numRight, rightType, rightOriginType, \
                           _output, _order)                                                                      \
  do {                                                                                                           \
    TYPE_CONVERT_DOUBLE_RES(left, right, _output, leftOriginType, rightOriginType, double);                      \
    ARRAY_LIST_MULTI(pLeft, pRight, leftType, rightType, numLeft, numRight, pOutput, _order);                    \
  } while (0)

#define DO_VECTOR_DIVIDE(left, numLeft, leftType, leftOriginType, right, numRight, rightType, rightOriginType, \
                         _output, _order)                                                                      \
  do {                                                                                                         \
    TYPE_CONVERT_DOUBLE_RES(left, right, _output, leftOriginType, rightOriginType, double);                    \
    ARRAY_LIST_DIV(pLeft, pRight, leftType, rightType, numLeft, numRight, pOutput, _order);                    \
  } while (0)

#define DO_VECTOR_REMAINDER(left, numLeft, leftType, leftOriginType, right, numRight, rightType, rightOriginType, \
                            _output, _order)                                                                      \
  do {                                                                                                            \
    TYPE_CONVERT_DOUBLE_RES(left, right, _output, leftOriginType, rightOriginType, double);                       \
    ARRAY_LIST_REM(pLeft, pRight, leftType, rightType, numLeft, numRight, pOutput, _order);                       \
  } while (0)

void calc_i32_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] + pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[0] + pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[i] + pRight[0];
    }
  }
}

void vectorAdd(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight, int32_t rightType,
               void *output, int32_t order) {
  switch(leftType) {
    case TSDB_DATA_TYPE_TINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, int8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, int16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, int32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, int64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, uint64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, float, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_ADD(left, numLeft, leftType, double, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    default:;
  }
}

void vectorSub(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight, int32_t rightType,
               void *output, int32_t order) {
  switch(leftType) {
    case TSDB_DATA_TYPE_TINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, int8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, int16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, int32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, int64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, uint64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, float, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_SUB(left, numLeft, leftType, double, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    default:;
  }
}

void vectorMultiply(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight, int32_t rightType,
               void *output, int32_t order) {
  switch(leftType) {
    case TSDB_DATA_TYPE_TINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, int64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, uint64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, float, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_MULTIPLY(left, numLeft, leftType, double, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    default:;
  }
}

void vectorDivide(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight, int32_t rightType,
                    void *output, int32_t order) {
  switch(leftType) {
    case TSDB_DATA_TYPE_TINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, int64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, uint64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, float, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_DIVIDE(left, numLeft, leftType, double, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    default:;
  }
}

void vectorRemainder(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight, int32_t rightType,
                    void *output, int32_t order) {
  switch(leftType) {
    case TSDB_DATA_TYPE_TINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint8_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint16_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint32_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, int64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, uint64_t, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, float, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      switch (rightType) {
        case TSDB_DATA_TYPE_TINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, int8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, uint8_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, int16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, uint16_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, int32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, uint32_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, int64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, uint64_t, output, order);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, float, output, order);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          DO_VECTOR_REMAINDER(left, numLeft, leftType, double, right, numRight, rightType, double, output, order);
          break;
        }
        default:
          assert(0);
      }
      break;
    }
    default:;
  }
}

/*
void calc_i32_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t);
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i32_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i32_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i8_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_u8_u8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, uint8_t, uint8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_UTINYINT, TSDB_DATA_TYPE_UTINYINT, numLeft, numRight, pOutput, order);
}

void calc_i8_u8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, uint8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_UTINYINT, numLeft, numRight, pOutput, order);
}

void calc_u8_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_u8_add(right, left, numRight, numLeft, output, order);
}

void calc_i8_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i8_add(right, left, numRight, numLeft, output, order);
}

void calc_i8_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i8_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i8_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i16_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_i16_add(right, left, numRight, numLeft, output, order);
}

void calc_i16_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i16_add(right, left, numRight, numLeft, output, order);
}

void calc_i16_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i16_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i16_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i64_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_i64_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_i64_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_i64_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i64_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i64_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_f_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_f_add(right, left, numRight, numLeft, output, order);
}

void calc_f_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_f_add(right, left, numRight, numLeft, output, order);
}

void calc_f_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_f_add(right, left, numRight, numLeft, output, order);
}

void calc_f_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i64_f_add(right, left, numRight, numLeft, output, order);
}

void calc_f_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_f_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_d_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_d_add(right, left, numRight, numLeft, output, order);
}

void calc_d_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_d_add(right, left, numRight, numLeft, output, order);
}

void calc_d_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_d_add(right, left, numRight, numLeft, output, order);
}

void calc_d_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i64_d_add(right, left, numRight, numLeft, output, order);
}

void calc_d_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_f_d_add(right, left, numRight, numLeft, output, order);
}

void calc_d_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_i32_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[i] - pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[0] - pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&pLeft[i], TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[i] - pRight[0];
    }
  }
}

void calc_i32_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i32_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i32_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i8_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i8_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i8_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i8_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i16_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i16_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i16_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i16_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i64_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i64_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i64_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i64_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_f_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_f_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_f_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_f_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_f_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_f_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_d_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_d_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_d_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_d_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_d_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_d_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_i32_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] * pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[0] * pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[i] * pRight[0];
    }
  }
}

void calc_i32_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i32_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i32_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i8_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i8_multi(right, left, numRight, numLeft, output, order);
}

void calc_i8_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i8_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i8_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i16_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_i16_multi(right, left, numRight, numLeft, output, order);
}

void calc_i16_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i16_multi(right, left, numRight, numLeft, output, order);
}

void calc_i16_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i16_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i16_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i64_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_i64_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_i64_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_i64_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i64_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i64_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_f_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_f_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_f_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_f_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i64_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_f_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_f_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_d_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i8_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_d_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i16_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_d_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i32_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_d_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_i64_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_d_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_f_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_d_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_i32_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] / pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[0] / pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }
      *pOutput = (double)pLeft[i] / pRight[0];
    }
  }
}

void calc_i32_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i32_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i32_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i8_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i8_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i8_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i8_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i16_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i16_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i16_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i16_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i64_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i64_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i64_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i64_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_f_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_f_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_f_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_f_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_f_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_f_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_d_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_d_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_d_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_d_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_d_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_d_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////

void calc_i32_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] - ((int64_t)(((double)pLeft[i]) / pRight[i])) * pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[0] - ((int64_t)(((double)pLeft[0]) / pRight[i])) * pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] - ((int64_t)(((double)pLeft[i]) / pRight[0])) * pRight[0];
    }
  }
}

void calc_i32_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i32_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i32_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i32_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  double * pRight = (double *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : MAX(numLeft, numRight) - 1;
  int32_t step = (order == TSDB_ORDER_ASC) ? 1 : -1;

  if (numLeft == numRight) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)(pOutput), TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] - ((int64_t)(((double)pLeft[i]) / pRight[i])) * pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[0] - ((int64_t)(((double)pLeft[0]) / pRight[i])) * pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        setNull((char *)pOutput, TSDB_DATA_TYPE_DOUBLE, tDataTypeDesc[TSDB_DATA_TYPE_DOUBLE].nSize);
        continue;
      }

      *pOutput = (double)pLeft[i] - ((int64_t)(((double)pLeft[i]) / pRight[0])) * pRight[0];
    }
  }
}

void calc_i8_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i8_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i8_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i8_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i8_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i16_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i16_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i16_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i16_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i16_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_i64_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_i64_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_i64_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_i64_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_i64_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_f_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_f_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_f_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_f_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_f_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_f_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_d_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_d_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_d_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_d_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_d_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_d_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

*
 * the following are two-dimensional array list of callback function .
 */
//_arithmetic_operator_fn_t add_function_arraylist[15][15] = {
//    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
//    {0},  // EMPTY,
//    {0},  // TSDB_DATA_TYPE_BOOL,
//    {NULL, NULL, calc_i8_i8_add, calc_i8_i16_add, calc_i8_i32_add, calc_i8_i64_add, calc_i8_f_add, calc_i8_d_add, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
//    {NULL, NULL, calc_i16_i8_add, calc_i16_i16_add, calc_i16_i32_add, calc_i16_i64_add, calc_i16_f_add, calc_i16_d_add, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
//    {NULL, NULL, calc_i32_i8_add, calc_i32_i16_add, calc_i32_i32_add, calc_i32_i64_add, calc_i32_f_add, calc_i32_d_add, NULL, NULL},  // TSDB_DATA_TYPE_INT
//    {NULL, NULL, calc_i64_i8_add, calc_i64_i16_add, calc_i64_i32_add, calc_i64_i64_add, calc_i64_f_add, calc_i64_d_add, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
//    {NULL, NULL, calc_f_i8_add, calc_f_i16_add, calc_f_i32_add, calc_f_i64_add, calc_f_f_add, calc_f_d_add, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
//    {NULL, NULL, calc_d_i8_add, calc_d_i16_add, calc_d_i32_add, calc_d_i64_add, calc_d_f_add, calc_d_d_add, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
//    {0},  // TSDB_DATA_TYPE_BINARY,
//    {0},  // TSDB_DATA_TYPE_NCHAR,
//    {NULL, NULL, calc_u8_i8_add, calc_u8_i16_add, calc_u8_i32_add, calc_u8_i64_add, calc_u8_f_add, calc_u8_d_add, NULL, NULL, calc_u8_u8_add, calc_u8_u16_add, calc_u8_u32_add, calc_u8_u64_add, NULL},  // TSDB_DATA_TYPE_UTINYINT,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_USMALLINT,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_UINT,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_UBIGINT,
//
//};
//
//_arithmetic_operator_fn_t sub_function_arraylist[8][15] = {
//    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
//    {0},  // EMPTY,
//    {0},  // TSDB_DATA_TYPE_BOOL,
//    {NULL, NULL, calc_i8_i8_sub, calc_i8_i16_sub, calc_i8_i32_sub, calc_i8_i64_sub, calc_i8_f_sub, calc_i8_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
//    {NULL, NULL, calc_i16_i8_sub, calc_i16_i16_sub, calc_i16_i32_sub, calc_i16_i64_sub, calc_i16_f_sub, calc_i16_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
//    {NULL, NULL, calc_i32_i8_sub, calc_i32_i16_sub, calc_i32_i32_sub, calc_i32_i64_sub, calc_i32_f_sub, calc_i32_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_INT
//    {NULL, NULL, calc_i64_i8_sub, calc_i64_i16_sub, calc_i64_i32_sub, calc_i64_i64_sub, calc_i64_f_sub, calc_i64_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
//    {NULL, NULL, calc_f_i8_sub, calc_f_i16_sub, calc_f_i32_sub, calc_f_i64_sub, calc_f_f_sub, calc_f_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
//    {NULL, NULL, calc_d_i8_sub, calc_d_i16_sub, calc_d_i32_sub, calc_d_i64_sub, calc_d_f_sub, calc_d_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
//};
//
//_arithmetic_operator_fn_t multi_function_arraylist[][15] = {
//    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
//    {NULL, NULL, calc_i8_i8_multi, calc_i8_i16_multi, calc_i8_i32_multi, calc_i8_i64_multi, calc_i8_f_multi, calc_i8_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
//    {NULL, NULL, calc_i16_i8_multi, calc_i16_i16_multi, calc_i16_i32_multi, calc_i16_i64_multi, calc_i16_f_multi, calc_i16_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
//    {NULL, NULL, calc_i32_i8_multi, calc_i32_i16_multi, calc_i32_i32_multi, calc_i32_i64_multi, calc_i32_f_multi, calc_i32_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_INT
//    {NULL, NULL, calc_i64_i8_multi, calc_i64_i16_multi, calc_i64_i32_multi, calc_i64_i64_multi, calc_i64_f_multi, calc_i64_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
//    {NULL, NULL, calc_f_i8_multi, calc_f_i16_multi, calc_f_i32_multi, calc_f_i64_multi, calc_f_f_multi, calc_f_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
//    {NULL, NULL, calc_d_i8_multi, calc_d_i16_multi, calc_d_i32_multi, calc_d_i64_multi, calc_d_f_multi, calc_d_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
//};
//
//_arithmetic_operator_fn_t div_function_arraylist[8][15] = {
//    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
//    {NULL, NULL, calc_i8_i8_div, calc_i8_i16_div, calc_i8_i32_div, calc_i8_i64_div, calc_i8_f_div, calc_i8_d_div, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
//    {NULL, NULL, calc_i16_i8_div, calc_i16_i16_div, calc_i16_i32_div, calc_i16_i64_div, calc_i16_f_div, calc_i16_d_div, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
//    {NULL, NULL, calc_i32_i8_div, calc_i32_i16_div, calc_i32_i32_div, calc_i32_i64_div, calc_i32_f_div, calc_i32_d_div, NULL, NULL},  // TSDB_DATA_TYPE_INT
//    {NULL, NULL, calc_i64_i8_div, calc_i64_i16_div, calc_i64_i32_div, calc_i64_i64_div, calc_i64_f_div, calc_i64_d_div, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
//    {NULL, NULL, calc_f_i8_div, calc_f_i16_div, calc_f_i32_div, calc_f_i64_div, calc_f_f_div, calc_f_d_div, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
//    {NULL, NULL, calc_d_i8_div, calc_d_i16_div, calc_d_i32_div, calc_d_i64_div, calc_d_f_div, calc_d_d_div, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
//};
//
//_arithmetic_operator_fn_t rem_function_arraylist[8][15] = {
//    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary, nchar, unsigned tinyint, unsigned smallint, unsigned int, unsigned bigint*/
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
//    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
//    {NULL, NULL, calc_i8_i8_rem, calc_i8_i16_rem, calc_i8_i32_rem, calc_i8_i64_rem, calc_i8_f_rem, calc_i8_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
//    {NULL, NULL, calc_i16_i8_rem, calc_i16_i16_rem, calc_i16_i32_rem, calc_i16_i64_rem, calc_i16_f_rem, calc_i16_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
//    {NULL, NULL, calc_i32_i8_rem, calc_i32_i16_rem, calc_i32_i32_rem, calc_i32_i64_rem, calc_i32_f_rem, calc_i32_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_INT
//    {NULL, NULL, calc_i64_i8_rem, calc_i64_i16_rem, calc_i64_i32_rem, calc_i64_i64_rem, calc_i64_f_rem, calc_i64_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
//    {NULL, NULL, calc_f_i8_rem, calc_f_i16_rem, calc_f_i32_rem, calc_f_i64_rem, calc_f_f_rem, calc_f_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
//    {NULL, NULL, calc_d_i8_rem, calc_d_i16_rem, calc_d_i32_rem, calc_d_i64_rem, calc_d_f_rem, calc_d_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
//};

////////////////////////////////////////////////////////////////////////////////////////////////////////////

_arithmetic_operator_fn_t getArithmeticOperatorFn(int32_t arithmeticOptr) {
  switch (arithmeticOptr) {
    case TSDB_BINARY_OP_ADD:
      return vectorAdd;
    case TSDB_BINARY_OP_SUBTRACT:
      return vectorSub;
    case TSDB_BINARY_OP_MULTIPLY:
      return vectorMultiply;
    case TSDB_BINARY_OP_DIVIDE:
      return vectorDivide;
    case TSDB_BINARY_OP_REMAINDER:
      return vectorRemainder;
    default:
      return NULL;
  }
}
