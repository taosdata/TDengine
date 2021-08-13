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

#include "ttype.h"
#include "tutil.h"
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

double getVectorDoubleValue(void *src, int32_t srcType, int32_t index) {
  switch (srcType) {
    case TSDB_DATA_TYPE_TINYINT: {
      return *((int8_t*)src + index);
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      return *((uint8_t*)src + index);
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      return *((int16_t*)src + index);
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      return *((uint16_t*)src + index);
    }
    case TSDB_DATA_TYPE_INT: {
      return *((int32_t*)src + index);
    }
    case TSDB_DATA_TYPE_UINT: {
      return *((uint32_t*)src + index);
    }
    case TSDB_DATA_TYPE_BIGINT: {
      return *((int64_t*)src + index);
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      return *((uint64_t*)src + index);
    }
    case TSDB_DATA_TYPE_FLOAT: {
      return *((float*)src + index);
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      return *((double*)src + index);
    }
    default:
      assert(0);
  }
}

void *getVectorValueAddr(void *src, int32_t srcType, int32_t index) {
  switch (srcType) {
    case TSDB_DATA_TYPE_TINYINT: {
      return (void*)((int8_t*)src + index);
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      return (void*)((uint8_t*)src + index);
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      return (void*)((int16_t*)src + index);
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      return (void*)((uint16_t*)src + index);
    }
    case TSDB_DATA_TYPE_INT: {
      return (void*)((int32_t*)src + index);
    }
    case TSDB_DATA_TYPE_UINT: {
      return (void*)((uint32_t*)src + index);
    }
    case TSDB_DATA_TYPE_BIGINT: {
      return (void*)((int64_t*)src + index);
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      return (void*)((uint64_t*)src + index);
    }
    case TSDB_DATA_TYPE_FLOAT: {
      return (void*)((float*)src + index);
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      return (void*)((double*)src + index);
    }
    default:
      assert(0);
  }
}

void vectorAdd(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=out;
                                                                                                 
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) + getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,0), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,0) + getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) + getVectorDoubleValue(right,_right_type,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorSub(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=out;
                                                                                                  
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) - getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,0), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,0) - getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) - getVectorDoubleValue(right,_right_type,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorMultiply(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=out;
                                                                                                  
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) * getVectorDoubleValue(right,_right_type,i));    
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,0), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,0) * getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) * getVectorDoubleValue(right,_right_type,0));                                                        
    }                                                                                               
  }                                                                                                 
}
void vectorDivide(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {                
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                 
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;                                               
  double *output=out;
                                                                                                    
  if ((len1) == (len2)) {                                                                           
    for (; i < (len2) && i >= 0; i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) { 
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) /getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len1) == 1) {                                                                         
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,0), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,0) /getVectorDoubleValue(right,_right_type,i));                                                       
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < (len1); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,0), _right_type)) {         
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,0));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) /getVectorDoubleValue(right,_right_type,0));                                                       
    }                                                                                               
  }                                                                                                 
} 
void vectorRemainder(void *left, int32_t len1, int32_t _left_type, void *right, int32_t len2, int32_t _right_type, void *out, int32_t _ord) {
  int32_t i = (_ord == TSDB_ORDER_ASC) ? 0 : MAX(len1, len2) - 1;                                   
  int32_t step = (_ord == TSDB_ORDER_ASC) ? 1 : -1;                                                 
  double *output=out;
                                                                                                    
  if (len1 == (len2)) {                                                                             
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {     
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) - ((int64_t)(getVectorDoubleValue(left,_left_type,i) / getVectorDoubleValue(right,_right_type,i))) * getVectorDoubleValue(right,_right_type,i));      
    }                                                                                               
  } else if (len1 == 1) {                                                                           
    for (; i >= 0 && i < (len2); i += step, output += 1) {                                           
      if (isNull(getVectorValueAddr(left,_left_type,0), _left_type) || isNull(getVectorValueAddr(right,_right_type,i), _right_type)) {       
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,i));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,0) - ((int64_t)(getVectorDoubleValue(left,_left_type,0) / getVectorDoubleValue(right,_right_type,i))) * getVectorDoubleValue(right,_right_type,i));      
    }                                                                                               
  } else if ((len2) == 1) {                                                                         
    for (; i >= 0 && i < len1; i += step, output += 1) {                                             
      if (isNull(getVectorValueAddr(left,_left_type,i), _left_type) || isNull(getVectorValueAddr(right,_right_type,0), _right_type)) {       
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      double v, z = 0.0;                                                                            
      GET_TYPED_DATA(v, double, _right_type, getVectorValueAddr(right,_right_type,0));                                
      if (getComparFunc(TSDB_DATA_TYPE_DOUBLE, 0)(&v, &z) == 0) {                                   
        SET_DOUBLE_NULL(output);                                                                       
        continue;                                                                                   
      }                                                                                             
      SET_DOUBLE_VAL(output,getVectorDoubleValue(left,_left_type,i) - ((int64_t)(getVectorDoubleValue(left,_left_type,i) / getVectorDoubleValue(right,_right_type,0))) * getVectorDoubleValue(right,_right_type,0));      
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
