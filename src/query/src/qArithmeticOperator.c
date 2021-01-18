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
#include "ttype.h"
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
        SET_DOUBLE_NULL(pOutput);
        continue;
      }

      *pOutput = (double)pLeft[i] + pRight[i];
    }
  } else if (numLeft == 1) {
    for (; i >= 0 && i < numRight; i += step, pOutput += 1) {
      if (isNull((char *)(pLeft), TSDB_DATA_TYPE_INT) || isNull((char *)&(pRight[i]), TSDB_DATA_TYPE_INT)) {
        SET_DOUBLE_NULL(pOutput);
        continue;
      }

      *pOutput = (double)pLeft[0] + pRight[i];
    }
  } else if (numRight == 1) {
    for (; i >= 0 && i < numLeft; i += step, pOutput += 1) {
      if (isNull((char *)&(pLeft[i]), TSDB_DATA_TYPE_INT) || isNull((char *)(pRight), TSDB_DATA_TYPE_INT)) {
        SET_DOUBLE_NULL(pOutput);
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
