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

#include "ttypes.h"
#include "tbinoperator.h"
#include "tcompare.h"

//GET_TYPED_DATA(v, double, pRight->type, (char *)&((right)[i]));                                

void calc_i32_i32_add(void *left, void *right, int32_t numLeft, int32_t numRight, void *output, int32_t order) {
  int32_t *pLeft = (int32_t *)left;
  int32_t *pRight = (int32_t *)right;
  double * pOutput = (double *)output;

  int32_t i = (order == TSDB_ORDER_ASC) ? 0 : TMAX(numLeft, numRight) - 1;
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

typedef double (*_getDoubleValue_fn_t)(void *src, int32_t index);

double getVectorDoubleValue_TINYINT(void *src, int32_t index) {
  return (double)*((int8_t *)src + index);
}
double getVectorDoubleValue_UTINYINT(void *src, int32_t index) {
  return (double)*((uint8_t *)src + index);
}
double getVectorDoubleValue_SMALLINT(void *src, int32_t index) {
  return (double)*((int16_t *)src + index);
}
double getVectorDoubleValue_USMALLINT(void *src, int32_t index) {
  return (double)*((uint16_t *)src + index);
}
double getVectorDoubleValue_INT(void *src, int32_t index) {
  return (double)*((int32_t *)src + index);
}
double getVectorDoubleValue_UINT(void *src, int32_t index) {
  return (double)*((uint32_t *)src + index);
}
double getVectorDoubleValue_BIGINT(void *src, int32_t index) {
  return (double)*((int64_t *)src + index);
}
double getVectorDoubleValue_UBIGINT(void *src, int32_t index) {
  return (double)*((uint64_t *)src + index);
}
double getVectorDoubleValue_FLOAT(void *src, int32_t index) {
  return (double)*((float *)src + index);
}
double getVectorDoubleValue_DOUBLE(void *src, int32_t index) {
  return (double)*((double *)src + index);
}
_getDoubleValue_fn_t getVectorDoubleValueFn(int32_t srcType) {
    _getDoubleValue_fn_t p = NULL;
    if(srcType==TSDB_DATA_TYPE_TINYINT) {
        p = getVectorDoubleValue_TINYINT;
    }else if(srcType==TSDB_DATA_TYPE_UTINYINT) {
        p = getVectorDoubleValue_UTINYINT;
    }else if(srcType==TSDB_DATA_TYPE_SMALLINT) {
        p = getVectorDoubleValue_SMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_USMALLINT) {
        p = getVectorDoubleValue_USMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_INT) {
        p = getVectorDoubleValue_INT;
    }else if(srcType==TSDB_DATA_TYPE_UINT) {
        p = getVectorDoubleValue_UINT;
    }else if(srcType==TSDB_DATA_TYPE_BIGINT) {
        p = getVectorDoubleValue_BIGINT;
    }else if(srcType==TSDB_DATA_TYPE_UBIGINT) {
        p = getVectorDoubleValue_UBIGINT;
    }else if(srcType==TSDB_DATA_TYPE_FLOAT) {
        p = getVectorDoubleValue_FLOAT;
    }else if(srcType==TSDB_DATA_TYPE_DOUBLE) {
        p = getVectorDoubleValue_DOUBLE;
    }else {
        assert(0);
    }
    return p;
}


typedef void* (*_getValueAddr_fn_t)(void *src, int32_t index);

void* getVectorValueAddr_TINYINT(void *src, int32_t index) {
  return (void*)((int8_t *)src + index);
}
void* getVectorValueAddr_UTINYINT(void *src, int32_t index) {
  return (void*)((uint8_t *)src + index);
}
void* getVectorValueAddr_SMALLINT(void *src, int32_t index) {
  return (void*)((int16_t *)src + index);
}
void* getVectorValueAddr_USMALLINT(void *src, int32_t index) {
  return (void*)((uint16_t *)src + index);
}
void* getVectorValueAddr_INT(void *src, int32_t index) {
  return (void*)((int32_t *)src + index);
}
void* getVectorValueAddr_UINT(void *src, int32_t index) {
  return (void*)((uint32_t *)src + index);
}
void* getVectorValueAddr_BIGINT(void *src, int32_t index) {
  return (void*)((int64_t *)src + index);
}
void* getVectorValueAddr_UBIGINT(void *src, int32_t index) {
  return (void*)((uint64_t *)src + index);
}
void* getVectorValueAddr_FLOAT(void *src, int32_t index) {
  return (void*)((float *)src + index);
}
void* getVectorValueAddr_DOUBLE(void *src, int32_t index) {
  return (void*)((double *)src + index);
}

_getValueAddr_fn_t getVectorValueAddrFn(int32_t srcType) {
    _getValueAddr_fn_t p = NULL;
    if(srcType==TSDB_DATA_TYPE_TINYINT) {
        p = getVectorValueAddr_TINYINT;
    }else if(srcType==TSDB_DATA_TYPE_UTINYINT) {
        p = getVectorValueAddr_UTINYINT;
    }else if(srcType==TSDB_DATA_TYPE_SMALLINT) {
        p = getVectorValueAddr_SMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_USMALLINT) {
        p = getVectorValueAddr_USMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_INT) {
        p = getVectorValueAddr_INT;
    }else if(srcType==TSDB_DATA_TYPE_UINT) {
        p = getVectorValueAddr_UINT;
    }else if(srcType==TSDB_DATA_TYPE_BIGINT) {
        p = getVectorValueAddr_BIGINT;
    }else if(srcType==TSDB_DATA_TYPE_UBIGINT) {
        p = getVectorValueAddr_UBIGINT;
    }else if(srcType==TSDB_DATA_TYPE_FLOAT) {
        p = getVectorValueAddr_FLOAT;
    }else if(srcType==TSDB_DATA_TYPE_DOUBLE) {
        p = getVectorValueAddr_DOUBLE;
    }else {
        assert(0);
    }
    return p;
}

void vectorAdd(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  double *output=(double*)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(pLeft->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      SET_DOUBLE_VAL(output, getVectorDoubleValueFnLeft(pLeft->data, i) + getVectorDoubleValueFnRight(pRight->data, i));
    }
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, 0), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data, 0) + getVectorDoubleValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,0), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data,i) + getVectorDoubleValueFnRight(pRight->data,0));
    }
  }
}

void vectorSub(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  double *output=(double*)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(pLeft->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }
      SET_DOUBLE_VAL(output, getVectorDoubleValueFnLeft(pLeft->data, i) - getVectorDoubleValueFnRight(pRight->data, i));
    }                                                                                               
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, 0), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data, 0) - getVectorDoubleValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,0), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data,i) - getVectorDoubleValueFnRight(pRight->data,0));
    }
  }
}
void vectorMultiply(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  double *output=(double*)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(pLeft->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      SET_DOUBLE_VAL(output, getVectorDoubleValueFnLeft(pLeft->data, i) * getVectorDoubleValueFnRight(pRight->data, i));
    }
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, 0), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data, 0) * getVectorDoubleValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,0), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(pLeft->data,i) * getVectorDoubleValueFnRight(pRight->data,0));
    }
  }                                                                                                 
}

void vectorDivide(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  double *output=(double*)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(pLeft->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      SET_DOUBLE_VAL(output, getVectorDoubleValueFnLeft(pLeft->data, i) / getVectorDoubleValueFnRight(pRight->data, i));
    }
  } else if (pLeft->num == 1) {
    double left = getVectorDoubleValueFnLeft(pLeft->data, 0);

    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(&left, pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      SET_DOUBLE_VAL(output,left / getVectorDoubleValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    double right = getVectorDoubleValueFnRight(pRight->data, 0);

    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(&right, pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      SET_DOUBLE_VAL(output, getVectorDoubleValueFnLeft(pLeft->data, i) / right);
    }
  }
}

void vectorRemainder(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  double *             output = (double *)out;
  _getValueAddr_fn_t   getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t   getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(pLeft->type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double v, u = 0.0;
      GET_TYPED_DATA(v, double, pRight->type, getVectorValueAddrFnRight(pRight->data, i));
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double left = getVectorDoubleValueFnLeft(pLeft->data, i);
      double right = getVectorDoubleValueFnRight(pRight->data, i);
      SET_DOUBLE_VAL(output, left - ((int64_t)(left / right)) * right);
    }
  } else if (pLeft->num == 1) {
    double left = getVectorDoubleValueFnLeft(pLeft->data, 0);

    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(&left, pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double v, u = 0.0;
      GET_TYPED_DATA(v, double, pRight->type, getVectorValueAddrFnRight(pRight->data, i));
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double right = getVectorDoubleValueFnRight(pRight->data, i);
      SET_DOUBLE_VAL(output, left - ((int64_t)(left / right)) * right);
    }
  } else if (pRight->num == 1) {
    double right = getVectorDoubleValueFnRight(pRight->data, 0);

    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(&right, pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double v, u = 0.0;
      GET_TYPED_DATA(v, double, pRight->type, getVectorValueAddrFnRight(pRight->data, 0));
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      double left = getVectorDoubleValueFnLeft(pLeft->data, i);
      SET_DOUBLE_VAL(output, left - ((int64_t)(left / right)) * right);
    }
  }                                                                                                 
}

void vectorConcat(SScalarFuncParam* pLeft, SScalarFuncParam* pRight, void *out, int32_t _ord) {
  int32_t len = pLeft->bytes + pRight->bytes;

  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  char *output = (char *)out;
  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += len) {
      char* left = POINTER_SHIFT(pLeft->data, pLeft->bytes * i);
      char* right = POINTER_SHIFT(pRight->data, pRight->bytes * i);

      if (isNull(left, pLeft->type) || isNull(right, pRight->type)) {
        setVardataNull(output, TSDB_DATA_TYPE_BINARY);
        continue;
      }

      // todo define a macro
      memcpy(varDataVal(output), varDataVal(left), varDataLen(left));
      memcpy(varDataVal(output) + varDataLen(left), varDataVal(right), varDataLen(right));
      varDataSetLen(output, varDataLen(left) + varDataLen(right));
    }
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += len) {
      char *right = POINTER_SHIFT(pRight->data, pRight->bytes * i);
      if (isNull(pLeft->data, pLeft->type) || isNull(right, pRight->type)) {
        setVardataNull(output, TSDB_DATA_TYPE_BINARY);
        continue;
      }

      memcpy(varDataVal(output), varDataVal(pLeft->data), varDataLen(pLeft->data));
      memcpy(varDataVal(output) + varDataLen(pLeft->data), varDataVal(right), varDataLen(right));
      varDataSetLen(output, varDataLen(pLeft->data) + varDataLen(right));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += len) {
      char* left = POINTER_SHIFT(pLeft->data, pLeft->bytes * i);
      if (isNull(left, pLeft->type) || isNull(pRight->data, pRight->type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      memcpy(varDataVal(output), varDataVal(left), varDataLen(pRight->data));
      memcpy(varDataVal(output) + varDataLen(left), varDataVal(pRight->data), varDataLen(pRight->data));
      varDataSetLen(output, varDataLen(left) + varDataLen(pRight->data));
    }
  }

}

_bin_scalar_fn_t getBinScalarOperatorFn(int32_t binFunctionId) {
  switch (binFunctionId) {
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
    case TSDB_BINARY_OP_CONCAT:
      return vectorConcat;
    default:
      assert(0);
      return NULL;
  }
}

bool isBinaryStringOp(int32_t op) {
  return op == TSDB_BINARY_OP_CONCAT;
}
