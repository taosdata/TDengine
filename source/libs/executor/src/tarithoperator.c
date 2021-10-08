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
#include "tarithoperator.h"
#include "tcompare.h"

//GET_TYPED_DATA(v, double, _right_type, (char *)&((right)[i]));                                

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

typedef double (*_arithmetic_getVectorDoubleValue_fn_t)(void *src, int32_t index);

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
_arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFn(int32_t srcType) {
    _arithmetic_getVectorDoubleValue_fn_t p = NULL;
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


typedef void* (*_arithmetic_getVectorValueAddr_fn_t)(void *src, int32_t index);

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

_arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFn(int32_t srcType) {
    _arithmetic_getVectorValueAddr_fn_t p = NULL;
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

void vectorAdd(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=(double*)out;
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(_left_type);
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(_right_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(_left_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(_right_type);
                                                                                                 
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) + getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,0), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,0) + getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) + getVectorDoubleValueFnRight(right,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorSub(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=(double*)out;
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(_left_type);
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(_right_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(_left_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(_right_type);
                                                                                                  
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) - getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,0), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,0) - getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) - getVectorDoubleValueFnRight(right,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorMultiply(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=(double*)out;
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(_left_type);
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(_right_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(_left_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(_right_type);
                                                                                                  
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) * getVectorDoubleValueFnRight(right,i));    
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,0), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,0) * getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) * getVectorDoubleValueFnRight(right,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorDivide(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {                
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=(double*)out;
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(_left_type);
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(_right_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(_left_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(_right_type);
                                                                                                    
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) /getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,0), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,0) /getVectorDoubleValueFnRight(right,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,0));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) /getVectorDoubleValueFnRight(right,0));                                                       
    }                                                                                               
  }                                                                                                 
} 
void vectorRemainder(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = (_ord == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                   
  int32_t step = (_ord == TSDB_ORDER_ASC) ? 1 : -1;                                                 
  double *output=(double*)out;
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(_left_type);
  _arithmetic_getVectorValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(_right_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnLeft = getVectorDoubleValueFn(_left_type);
  _arithmetic_getVectorDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(_right_type);
                                                                                                    
  if (len1 == (len2)) {                                                                             
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {     
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) - ((int64_t)(getVectorDoubleValueFnLeft(left,i) / getVectorDoubleValueFnRight(right,i))) * getVectorDoubleValueFnRight(right,i));      
    }                                                                                               
  } else if (len1 == 1) {                                                                           
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddrFnLeft(left,0), _left_type) || isNull(getVectorValueAddrFnRight(right,i), _right_type)) {       
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,0) - ((int64_t)(getVectorDoubleValueFnLeft(left,0) / getVectorDoubleValueFnRight(right,i))) * getVectorDoubleValueFnRight(right,i));      
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < len1; i += step, output += 1) {                                             
      if (isNull(getVectorValueAddrFnLeft(left,i), _left_type) || isNull(getVectorValueAddrFnRight(right,0), _right_type)) {       
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, u = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddrFnRight(right,0));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &u) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValueFnLeft(left,i) - ((int64_t)(getVectorDoubleValueFnLeft(left,i) / getVectorDoubleValueFnRight(right,0))) * getVectorDoubleValueFnRight(right,0));      
    }                                                                                               
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
      assert(0);
      return NULL;
  }
}
