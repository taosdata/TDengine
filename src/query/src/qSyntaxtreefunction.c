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

#include "qSyntaxtreefunction.h"
#include "taosdef.h"
#include "tutil.h"

#define ARRAY_LIST_OP(left, right, _left_type, _right_type, len1, len2, out, op, _res_type, _ord)     \
  {                                                                                                   \
    int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                    \
    int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                                  \
                                                                                                      \
    if ((len1) == (len2)) {                                                                           \
      for (; i < (len2) && i >= 0; i += step, (out) += 1) {                                        \
        if (isNull((char *)&((left)[i]), _left_type) || isNull((char *)&((right)[i]), _right_type)) { \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] op(right)[i];                                                      \
      }                                                                                               \
    } else if ((len1) == 1) {                                                                         \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                        \
        if (isNull((char *)(left), _left_type) || isNull((char *)&(right)[i], _right_type)) {         \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[0] op(right)[i];                                                      \
      }                                                                                               \
    } else if ((len2) == 1) {                                                                         \
      for (; i >= 0 && i < (len1); i += step, (out) += 1) {                                        \
        if (isNull((char *)&(left)[i], _left_type) || isNull((char *)(right), _right_type)) {         \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] op(right)[0];                                                      \
      }                                                                                               \
    }                                                                                                 \
  }

#define ARRAY_LIST_OP_REM(left, right, _left_type, _right_type, len1, len2, out, op, _res_type, _ord) \
  {                                                                                                   \
    int32_t i = (_ord == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                      \
    int32_t step = (_ord == TSDB_ORDER_ASC) ? 1 : -1;                                                    \
                                                                                                      \
    if (len1 == (len2)) {                                                                             \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                        \
        if (isNull((char *)&(left[i]), _left_type) || isNull((char *)&(right[i]), _right_type)) {     \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[i] - ((int64_t)(((double)(left)[i]) / (right)[i])) * (right)[i];      \
      }                                                                                               \
    } else if (len1 == 1) {                                                                           \
      for (; i >= 0 && i < (len2); i += step, (out) += 1) {                                        \
        if (isNull((char *)(left), _left_type) || isNull((char *)&((right)[i]), _right_type)) {       \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
          continue;                                                                                   \
        }                                                                                             \
        *(out) = (double)(left)[0] - ((int64_t)(((double)(left)[0]) / (right)[i])) * (right)[i];      \
      }                                                                                               \
    } else if ((len2) == 1) {                                                                         \
      for (; i >= 0 && i < len1; i += step, (out) += 1) {                                          \
        if (isNull((char *)&((left)[i]), _left_type) || isNull((char *)(right), _right_type)) {       \
          setNull((char *)(out), _res_type, tDataTypeDesc[_res_type].nSize);                          \
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

#define TYPE_CONVERT(left, right, out, _type_left, _type_right) \
  TYPE_CONVERT_DOUBLE_RES(left, right, out, _type_left, _type_right, double)

void calc_fn_i32_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i32_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t);
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i8_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i8_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_i16_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i16_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i16_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i16_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i64_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_f_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_f_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_f_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i64_f_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i8_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_d_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i16_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_d_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_d_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i64_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i64_d_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_f_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_f_d_add(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_d_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_ADD(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_fn_i32_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i32_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i8_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i16_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i32_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i64_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_f_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_d_sub(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_SUB(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_fn_i32_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i32_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i8_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i8_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_i16_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i16_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i16_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i16_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_i64_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_i64_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i64_f_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_f_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i8_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i8_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i16_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i16_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i32_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i32_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_i64_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_i64_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_f_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  calc_fn_f_d_multi(right, left, numRight, numLeft, output, order);
}

void calc_fn_d_d_multi(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_MULTI(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
void calc_fn_i32_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i32_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i8_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i16_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i32_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i64_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_f_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_d_div(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_DIV(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////

void calc_fn_i32_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i32_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int32_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i32_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
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

void calc_fn_i8_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i8_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int8_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_TINYINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i16_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int16_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_SMALLINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_i64_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, int64_t, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_BIGINT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_f_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, float, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_FLOAT, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i8_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int8_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_TINYINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i16_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int16_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_SMALLINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i32_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int32_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_INT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_i64_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, int64_t)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_BIGINT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_f_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, float)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_FLOAT, numLeft, numRight, pOutput, order);
}

void calc_fn_d_d_rem(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  TYPE_CONVERT(left, right, output, double, double)
  ARRAY_LIST_REM(pLeft, pRight, TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, numLeft, numRight, pOutput, order);
}

/*
 * the following are two-dimensional array list of callback function .
 */
_bi_consumer_fn_t add_function_arraylist[8][10] = {
    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
    {NULL, NULL, calc_fn_i8_i8_add, calc_fn_i8_i16_add, calc_fn_i8_i32_add, calc_fn_i8_i64_add, calc_fn_i8_f_add, calc_fn_i8_d_add, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
    {NULL, NULL, calc_fn_i16_i8_add, calc_fn_i16_i16_add, calc_fn_i16_i32_add, calc_fn_i16_i64_add, calc_fn_i16_f_add, calc_fn_i16_d_add, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
    {NULL, NULL, calc_fn_i32_i8_add, calc_fn_i32_i16_add, calc_fn_i32_i32_add, calc_fn_i32_i64_add, calc_fn_i32_f_add, calc_fn_i32_d_add, NULL, NULL},  // TSDB_DATA_TYPE_INT
    {NULL, NULL, calc_fn_i64_i8_add, calc_fn_i64_i16_add, calc_fn_i64_i32_add, calc_fn_i64_i64_add, calc_fn_i64_f_add, calc_fn_i64_d_add, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
    {NULL, NULL, calc_fn_f_i8_add, calc_fn_f_i16_add, calc_fn_f_i32_add, calc_fn_f_i64_add, calc_fn_f_f_add, calc_fn_f_d_add, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
    {NULL, NULL, calc_fn_d_i8_add, calc_fn_d_i16_add, calc_fn_d_i32_add, calc_fn_d_i64_add, calc_fn_d_f_add, calc_fn_d_d_add, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
};

_bi_consumer_fn_t sub_function_arraylist[8][10] = {
    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
    {NULL, NULL, calc_fn_i8_i8_sub, calc_fn_i8_i16_sub, calc_fn_i8_i32_sub, calc_fn_i8_i64_sub, calc_fn_i8_f_sub, calc_fn_i8_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
    {NULL, NULL, calc_fn_i16_i8_sub, calc_fn_i16_i16_sub, calc_fn_i16_i32_sub, calc_fn_i16_i64_sub, calc_fn_i16_f_sub, calc_fn_i16_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
    {NULL, NULL, calc_fn_i32_i8_sub, calc_fn_i32_i16_sub, calc_fn_i32_i32_sub, calc_fn_i32_i64_sub, calc_fn_i32_f_sub, calc_fn_i32_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_INT
    {NULL, NULL, calc_fn_i64_i8_sub, calc_fn_i64_i16_sub, calc_fn_i64_i32_sub, calc_fn_i64_i64_sub, calc_fn_i64_f_sub, calc_fn_i64_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
    {NULL, NULL, calc_fn_f_i8_sub, calc_fn_f_i16_sub, calc_fn_f_i32_sub, calc_fn_f_i64_sub, calc_fn_f_f_sub, calc_fn_f_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
    {NULL, NULL, calc_fn_d_i8_sub, calc_fn_d_i16_sub, calc_fn_d_i32_sub, calc_fn_d_i64_sub, calc_fn_d_f_sub, calc_fn_d_d_sub, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
};

_bi_consumer_fn_t multi_function_arraylist[][10] = {
    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
    {NULL, NULL, calc_fn_i8_i8_multi, calc_fn_i8_i16_multi, calc_fn_i8_i32_multi, calc_fn_i8_i64_multi, calc_fn_i8_f_multi, calc_fn_i8_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
    {NULL, NULL, calc_fn_i16_i8_multi, calc_fn_i16_i16_multi, calc_fn_i16_i32_multi, calc_fn_i16_i64_multi, calc_fn_i16_f_multi, calc_fn_i16_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
    {NULL, NULL, calc_fn_i32_i8_multi, calc_fn_i32_i16_multi, calc_fn_i32_i32_multi, calc_fn_i32_i64_multi, calc_fn_i32_f_multi, calc_fn_i32_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_INT
    {NULL, NULL, calc_fn_i64_i8_multi, calc_fn_i64_i16_multi, calc_fn_i64_i32_multi, calc_fn_i64_i64_multi, calc_fn_i64_f_multi, calc_fn_i64_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
    {NULL, NULL, calc_fn_f_i8_multi, calc_fn_f_i16_multi, calc_fn_f_i32_multi, calc_fn_f_i64_multi, calc_fn_f_f_multi, calc_fn_f_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
    {NULL, NULL, calc_fn_d_i8_multi, calc_fn_d_i16_multi, calc_fn_d_i32_multi, calc_fn_d_i64_multi, calc_fn_d_f_multi, calc_fn_d_d_multi, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
};

_bi_consumer_fn_t div_function_arraylist[8][10] = {
    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
    {NULL, NULL, calc_fn_i8_i8_div, calc_fn_i8_i16_div, calc_fn_i8_i32_div, calc_fn_i8_i64_div, calc_fn_i8_f_div, calc_fn_i8_d_div, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
    {NULL, NULL, calc_fn_i16_i8_div, calc_fn_i16_i16_div, calc_fn_i16_i32_div, calc_fn_i16_i64_div, calc_fn_i16_f_div, calc_fn_i16_d_div, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
    {NULL, NULL, calc_fn_i32_i8_div, calc_fn_i32_i16_div, calc_fn_i32_i32_div, calc_fn_i32_i64_div, calc_fn_i32_f_div, calc_fn_i32_d_div, NULL, NULL},  // TSDB_DATA_TYPE_INT
    {NULL, NULL, calc_fn_i64_i8_div, calc_fn_i64_i16_div, calc_fn_i64_i32_div, calc_fn_i64_i64_div, calc_fn_i64_f_div, calc_fn_i64_d_div, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
    {NULL, NULL, calc_fn_f_i8_div, calc_fn_f_i16_div, calc_fn_f_i32_div, calc_fn_f_i64_div, calc_fn_f_f_div, calc_fn_f_d_div, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
    {NULL, NULL, calc_fn_d_i8_div, calc_fn_d_i16_div, calc_fn_d_i32_div, calc_fn_d_i64_div, calc_fn_d_f_div, calc_fn_d_d_div, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
};

_bi_consumer_fn_t rem_function_arraylist[8][10] = {
    /*NULL, bool, tinyint, smallint, int, bigint, float, double, timestamp, binary*/
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // EMPTY,
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL},  // TSDB_DATA_TYPE_BOOL,
    {NULL, NULL, calc_fn_i8_i8_rem, calc_fn_i8_i16_rem, calc_fn_i8_i32_rem, calc_fn_i8_i64_rem, calc_fn_i8_f_rem, calc_fn_i8_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_TINYINT
    {NULL, NULL, calc_fn_i16_i8_rem, calc_fn_i16_i16_rem, calc_fn_i16_i32_rem, calc_fn_i16_i64_rem, calc_fn_i16_f_rem, calc_fn_i16_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_SMALLINT
    {NULL, NULL, calc_fn_i32_i8_rem, calc_fn_i32_i16_rem, calc_fn_i32_i32_rem, calc_fn_i32_i64_rem, calc_fn_i32_f_rem, calc_fn_i32_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_INT
    {NULL, NULL, calc_fn_i64_i8_rem, calc_fn_i64_i16_rem, calc_fn_i64_i32_rem, calc_fn_i64_i64_rem, calc_fn_i64_f_rem, calc_fn_i64_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_BIGINT
    {NULL, NULL, calc_fn_f_i8_rem, calc_fn_f_i16_rem, calc_fn_f_i32_rem, calc_fn_f_i64_rem, calc_fn_f_f_rem, calc_fn_f_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_FLOAT
    {NULL, NULL, calc_fn_d_i8_rem, calc_fn_d_i16_rem, calc_fn_d_i32_rem, calc_fn_d_i64_rem, calc_fn_d_f_rem, calc_fn_d_d_rem, NULL, NULL},  // TSDB_DATA_TYPE_DOUBLE
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////

_bi_consumer_fn_t tGetBiConsumerFn(int32_t leftType, int32_t rightType, int32_t optr) {
  switch (optr) {
    case TSDB_BINARY_OP_ADD:
      return add_function_arraylist[leftType][rightType];
    case TSDB_BINARY_OP_SUBTRACT:
      return sub_function_arraylist[leftType][rightType];
    case TSDB_BINARY_OP_MULTIPLY:
      return multi_function_arraylist[leftType][rightType];
    case TSDB_BINARY_OP_DIVIDE:
      return div_function_arraylist[leftType][rightType];
    case TSDB_BINARY_OP_REMAINDER:
      return rem_function_arraylist[leftType][rightType];
    default:
      assert(0);
      return NULL;
  }

  assert(0);
  return NULL;
}
