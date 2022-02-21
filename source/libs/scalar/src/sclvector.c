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
#include "sclvector.h"
#include "tcompare.h"
#include "querynodes.h"
#include "filterInt.h"
#include "query.h"

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



typedef int64_t (*_getBigintValue_fn_t)(void *src, int32_t index);

int64_t getVectorBigintValue_TINYINT(void *src, int32_t index) {
  return (int64_t)*((int8_t *)src + index);
}
int64_t getVectorBigintValue_UTINYINT(void *src, int32_t index) {
  return (int64_t)*((uint8_t *)src + index);
}
int64_t getVectorBigintValue_SMALLINT(void *src, int32_t index) {
  return (int64_t)*((int16_t *)src + index);
}
int64_t getVectorBigintValue_USMALLINT(void *src, int32_t index) {
  return (int64_t)*((uint16_t *)src + index);
}
int64_t getVectorBigintValue_INT(void *src, int32_t index) {
  return (int64_t)*((int32_t *)src + index);
}
int64_t getVectorBigintValue_UINT(void *src, int32_t index) {
  return (int64_t)*((uint32_t *)src + index);
}
int64_t getVectorBigintValue_BIGINT(void *src, int32_t index) {
  return (int64_t)*((int64_t *)src + index);
}
int64_t getVectorBigintValue_UBIGINT(void *src, int32_t index) {
  return (int64_t)*((uint64_t *)src + index);
}
int64_t getVectorBigintValue_FLOAT(void *src, int32_t index) {
  return (int64_t)*((float *)src + index);
}
int64_t getVectorBigintValue_DOUBLE(void *src, int32_t index) {
  return (int64_t)*((double *)src + index);
}
_getBigintValue_fn_t getVectorBigintValueFn(int32_t srcType) {
    _getBigintValue_fn_t p = NULL;
    if(srcType==TSDB_DATA_TYPE_TINYINT) {
        p = getVectorBigintValue_TINYINT;
    }else if(srcType==TSDB_DATA_TYPE_UTINYINT) {
        p = getVectorBigintValue_UTINYINT;
    }else if(srcType==TSDB_DATA_TYPE_SMALLINT) {
        p = getVectorBigintValue_SMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_USMALLINT) {
        p = getVectorBigintValue_USMALLINT;
    }else if(srcType==TSDB_DATA_TYPE_INT) {
        p = getVectorBigintValue_INT;
    }else if(srcType==TSDB_DATA_TYPE_UINT) {
        p = getVectorBigintValue_UINT;
    }else if(srcType==TSDB_DATA_TYPE_BIGINT) {
        p = getVectorBigintValue_BIGINT;
    }else if(srcType==TSDB_DATA_TYPE_UBIGINT) {
        p = getVectorBigintValue_UBIGINT;
    }else if(srcType==TSDB_DATA_TYPE_FLOAT) {
        p = getVectorBigintValue_FLOAT;
    }else if(srcType==TSDB_DATA_TYPE_DOUBLE) {
        p = getVectorBigintValue_DOUBLE;
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


int32_t vectorConvertImpl(SScalarParam* pIn, SScalarParam* pOut) {
  int16_t inType = pIn->type; 
  int16_t inBytes = pIn->bytes;
  char *input = pIn->data; 
  int16_t outType = pOut->type; 
  int16_t outBytes = pOut->bytes; 
  char *output = pOut->data;

  switch (outType) {
    case TSDB_DATA_TYPE_BOOL:
      if (inType == TSDB_DATA_TYPE_BINARY) {
        for (int32_t i = 0; i < pIn->num; ++i) {
          GET_TYPED_DATA(*(bool *)output, bool, TSDB_DATA_TYPE_USMALLINT, &varDataLen(input));
          
          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += sizeof(bool);
        }
      } else if (inType == TSDB_DATA_TYPE_NCHAR) {      
        for (int32_t i = 0; i < pIn->num; ++i) {
          GET_TYPED_DATA(*(bool *)output, bool, TSDB_DATA_TYPE_USMALLINT, &varDataLen(input));
          
          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
      } else {
        for (int32_t i = 0; i < pIn->num; ++i) {
          uint64_t value = 0;
          GET_TYPED_DATA(value, uint64_t, inType, input);
          SET_TYPED_DATA(output, outType, value);

          input += tDataTypes[inType].bytes;
          output += tDataTypes[outType].bytes;
        }
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:    
      if (inType == TSDB_DATA_TYPE_BINARY) {
        int32_t bufSize = varDataLen(input) + 1;
        char *tmp = malloc(bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input) >= bufSize) {
            bufSize = varDataLen(input) + 1;
            tmp = realloc(tmp, bufSize);
          }
          
          memcpy(tmp, varDataVal(input), varDataLen(input));
          tmp[varDataLen(input)] = 0;
          
          int64_t value = strtoll(tmp, NULL, 10);
          SET_TYPED_DATA(output, outType, value);
          
          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else if (inType == TSDB_DATA_TYPE_NCHAR) {      
        int32_t bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
        char *tmp = calloc(1, bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input)* TSDB_NCHAR_SIZE >= bufSize) {
            bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
            tmp = realloc(tmp, bufSize);
          }
        
          int len = taosUcs4ToMbs(varDataVal(input), varDataLen(input), tmp);
          if (len < 0){
            qError("castConvert taosUcs4ToMbs error 1");
            tfree(tmp);
            return TSDB_CODE_QRY_APP_ERROR;
          }
          
          tmp[len] = 0;
          int64_t value = strtoll(tmp, NULL, 10);
          SET_TYPED_DATA(output, outType, value);

          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else {
        for (int32_t i = 0; i < pIn->num; ++i) {
          int64_t value = 0;
          GET_TYPED_DATA(value, int64_t, inType, input);
          SET_TYPED_DATA(output, outType, value);

          input += tDataTypes[inType].bytes;
          output += tDataTypes[outType].bytes;
        }
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      if (inType == TSDB_DATA_TYPE_BINARY) {
        int32_t bufSize = varDataLen(input) + 1;
        char *tmp = malloc(bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input) >= bufSize) {
            bufSize = varDataLen(input) + 1;
            tmp = realloc(tmp, bufSize);
          }
          
          memcpy(tmp, varDataVal(input), varDataLen(input));
          tmp[varDataLen(input)] = 0;
          uint64_t value = strtoull(tmp, NULL, 10);
          SET_TYPED_DATA(output, outType, value);
          
          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else if (inType == TSDB_DATA_TYPE_NCHAR) {      
        int32_t bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
        char *tmp = calloc(1, bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input)* TSDB_NCHAR_SIZE >= bufSize) {
            bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
            tmp = realloc(tmp, bufSize);
          }
        
          int len = taosUcs4ToMbs(varDataVal(input), varDataLen(input), tmp);
          if (len < 0){
            qError("castConvert taosUcs4ToMbs error 1");
            tfree(tmp);
            return TSDB_CODE_QRY_APP_ERROR;
          }
          
          tmp[len] = 0;
          uint64_t value = strtoull(tmp, NULL, 10);
          SET_TYPED_DATA(output, outType, value);

          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else {      
        for (int32_t i = 0; i < pIn->num; ++i) {
          uint64_t value = 0;
          GET_TYPED_DATA(value, uint64_t, inType, input);
          SET_TYPED_DATA(output, outType, value);

          input += tDataTypes[inType].bytes;
          output += tDataTypes[outType].bytes;
        }
      }
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      if (inType == TSDB_DATA_TYPE_BINARY) {
        int32_t bufSize = varDataLen(input) + 1;
        char *tmp = malloc(bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input) >= bufSize) {
            bufSize = varDataLen(input) + 1;
            tmp = realloc(tmp, bufSize);
          }
          
          memcpy(tmp, varDataVal(input), varDataLen(input));
          tmp[varDataLen(input)] = 0;
          
          double value = strtod(tmp, NULL);
          SET_TYPED_DATA(output, outType, value);
          
          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else if (inType == TSDB_DATA_TYPE_NCHAR) {      
        int32_t bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
        char *tmp = calloc(1, bufSize);
        if (NULL == tmp) {
          return TSDB_CODE_QRY_OUT_OF_MEMORY;
        }
        
        for (int32_t i = 0; i < pIn->num; ++i) {
          if (varDataLen(input)* TSDB_NCHAR_SIZE >= bufSize) {
            bufSize = varDataLen(input) * TSDB_NCHAR_SIZE + 1;
            tmp = realloc(tmp, bufSize);
          }
        
          int len = taosUcs4ToMbs(varDataVal(input), varDataLen(input), tmp);
          if (len < 0){
            qError("castConvert taosUcs4ToMbs error 1");
            tfree(tmp);
            return TSDB_CODE_QRY_APP_ERROR;
          }
          
          tmp[len] = 0;
          double value = strtod(tmp, NULL);
          SET_TYPED_DATA(output, outType, value);

          input += varDataLen(input) + VARSTR_HEADER_SIZE;
          output += tDataTypes[outType].bytes;
        }
        
        tfree(tmp);
      } else {
        for (int32_t i = 0; i < pIn->num; ++i) {
          int64_t value = 0;
          GET_TYPED_DATA(value, int64_t, inType, input);
          SET_TYPED_DATA(output, outType, value);

          input += tDataTypes[inType].bytes;
          output += tDataTypes[outType].bytes;
        }
      }
      break;      
    default:
      qError("invalid convert output type:%d", outType);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int8_t gConvertTypes[TSDB_DATA_TYPE_BLOB+1][TSDB_DATA_TYPE_BLOB+1] = {
/*         NULL BOOL TINY SMAL INT  BIG  FLOA DOUB BINA TIME NCHA UTIN USMA UINT UBIG VARC VARB JSON DECI BLOB */
/*NULL*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*BOOL*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   0,   12,  13,  14,  7,   7,   0,   0,   0,
/*TINY*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   7,   7,   0,   0,   0,
/*SMAL*/   0,   0,   0,   0,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   7,   7,   0,   0,   0,
/*INT */   0,   0,   0,   0,   0,   5,   6,   7,   7,   9,   7,   4,   4,   5,   7,   7,   7,   0,   0,   0,
/*BIGI*/   0,   0,   0,   0,   0,   0,   6,   7,   7,   0,   7,   5,   5,   5,   7,   7,   7,   0,   0,   0,
/*FLOA*/   0,   0,   0,   0,   0,   0,   0,   7,   7,   6,   7,   6,   6,   6,   6,   7,   7,   0,   0,   0,
/*DOUB*/   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   7,   7,   7,   7,   7,   0,   0,   0,
/*BINA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   0,   7,   7,   7,   7,   0,   0,   0,   0,   0,
/*TIME*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   9,   9,   9,   7,   7,   7,   0,   0,   0,
/*NCHA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   0,   0,   0,   0,   0,
/*UTIN*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   12,  13,  14,  7,   7,   0,   0,   0,
/*USMA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   13,  14,  7,   7,   0,   0,   0,
/*UINT*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   14,  7,   7,   0,   0,   0,
/*UBIG*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   0,   0,   0,
/*VARC*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*VARB*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*JSON*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*DECI*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*BLOB*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0
};

int32_t vectorGetConvertType(int32_t type1, int32_t type2) {
  if (type1 == type2) {
    return 0;
  }

  if (type1 < type2) {
    return gConvertTypes[type1][type2];
  }

  return gConvertTypes[type2][type1];
}

int32_t vectorConvert(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam* pLeftOut, SScalarParam* pRightOut) {
  if (pLeft->type == pRight->type) {
    return TSDB_CODE_SUCCESS;
  }

  SScalarParam *param1 = NULL, *paramOut1 = NULL; 
  SScalarParam *param2 = NULL, *paramOut2 = NULL;
  int32_t code = 0;
  
  if (pLeft->type < pRight->type) {
    param1 = pLeft;
    param2 = pRight;
    paramOut1 = pLeftOut;
    paramOut2 = pRightOut;
  } else {
    param1 = pRight;
    param2 = pLeft;
    paramOut1 = pRightOut;
    paramOut2 = pLeftOut;
  }


  int8_t type = vectorGetConvertType(param1->type, param2->type);
  if (0 == type) {
    return TSDB_CODE_SUCCESS;
  }

  if (type != param1->type) {
    paramOut1->bytes = param1->bytes;
    paramOut1->type = type;
    paramOut1->num = param1->num;
    paramOut1->data = malloc(paramOut1->num * tDataTypes[paramOut1->type].bytes);
    if (NULL == paramOut1->data) {
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
    
    code = vectorConvertImpl(param1, paramOut1);
    if (code) {
      tfree(paramOut1->data);
      return code;
    }
  }
  
  if (type != param2->type) {
    paramOut2->bytes = param2->bytes;
    paramOut2->type = type;
    paramOut2->num = param2->num;
    paramOut2->data = malloc(paramOut2->num * tDataTypes[paramOut2->type].bytes);
    if (NULL == paramOut2->data) {
      tfree(paramOut1->data);
      return TSDB_CODE_QRY_OUT_OF_MEMORY;
    }
    
    code = vectorConvertImpl(param2, paramOut2);
    if (code) {
      tfree(paramOut1->data);
      tfree(paramOut2->data);
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void vectorAdd(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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

void vectorSub(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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
void vectorMultiply(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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

void vectorDivide(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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

void vectorRemainder(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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

void vectorConcat(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
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


void vectorBitAnd(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  int64_t *output=(int64_t *)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getBigintValue_fn_t getVectorBigintValueFnLeft = getVectorBigintValueFn(pLeft->type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }

      SET_BIGINT_VAL(output, getVectorBigintValueFnLeft(pLeft->data, i) & getVectorBigintValueFnRight(pRight->data, i));
    }
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, 0), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }
      SET_BIGINT_VAL(output,getVectorBigintValueFnLeft(pLeft->data, 0) & getVectorBigintValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,0), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }
      SET_BIGINT_VAL(output,getVectorBigintValueFnLeft(pLeft->data,i) & getVectorBigintValueFnRight(pRight->data,0));
    }
  }
}

void vectorBitOr(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  int64_t *output=(int64_t *)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);
  _getBigintValue_fn_t getVectorBigintValueFnLeft = getVectorBigintValueFn(pLeft->type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRight->type);

  if (pLeft->num == pRight->num) {
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }

      SET_BIGINT_VAL(output, getVectorBigintValueFnLeft(pLeft->data, i) | getVectorBigintValueFnRight(pRight->data, i));
    }
  } else if (pLeft->num == 1) {
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, 0), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }
      SET_BIGINT_VAL(output,getVectorBigintValueFnLeft(pLeft->data, 0) | getVectorBigintValueFnRight(pRight->data,i));
    }
  } else if (pRight->num == 1) {
    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,0), pRight->type)) {
        SET_BIGINT_NULL(output);
        continue;
      }
      SET_BIGINT_VAL(output,getVectorBigintValueFnLeft(pLeft->data,i) | getVectorBigintValueFnRight(pRight->data,0));
    }
  }
}


void vectorCompareImpl(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord, int32_t optr) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;
  __compar_fn_t fp = filterGetCompFunc(pLeft->type, optr);
  bool res = false;
  
  bool *output=(bool *)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);
  _getValueAddr_fn_t getVectorValueAddrFnRight = getVectorValueAddrFn(pRight->type);

  if (pLeft->num == pRight->num) {    
    for (; i < pRight->num && i >= 0; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data, i), pLeft->type) ||
          isNull(getVectorValueAddrFnRight(pRight->data, i), pRight->type)) {
        res = false;
        SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
        continue;
      }
      
      res = filterDoCompare(fp, optr, getVectorValueAddrFnLeft(pLeft->data, i), getVectorValueAddrFnRight(pRight->data,i));

      SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
    }
  } else if (pLeft->num == 1) {
    void *leftData = getVectorValueAddrFnLeft(pLeft->data, 0);
    
    for (; i >= 0 && i < pRight->num; i += step, output += 1) {
      if (isNull(leftData, pLeft->type) || isNull(getVectorValueAddrFnRight(pRight->data,i), pRight->type)) {
        res = false;
        SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
        continue;
      }

      res = filterDoCompare(fp, optr, leftData, getVectorValueAddrFnRight(pRight->data,i));

      SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
    }
  } else if (pRight->num == 1) {
      void *rightData = getVectorValueAddrFnRight(pRight->data, 0);

    for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
      if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type) || isNull(rightData, pRight->type)) {
        res = false;
        SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
        continue;
      }

      res = filterDoCompare(fp, optr, getVectorValueAddrFnLeft(pLeft->data,i), rightData);

      SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
    }
  }
}

void vectorCompare(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord, int32_t optr) {
  SScalarParam pLeftOut = {0}; 
  SScalarParam pRightOut = {0};
  
  vectorConvert(pLeft, pRight, &pLeftOut, &pRightOut);

  SScalarParam *param1 = NULL; 
  SScalarParam *param2 = NULL;

  int32_t type = 0;
  if (pLeftOut.type) {
    param1 = &pLeftOut;
  } else {
    param1 = pLeft;
  }

  if (pRightOut.type) {
    param2 = &pRightOut;
  } else {
    param2 = pRight;
  }

  vectorCompareImpl(param1, param2, out, _ord, TSDB_RELATION_GREATER);
}

void vectorGreater(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_GREATER);
}

void vectorGreaterEqual(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_GREATER_EQUAL);
}

void vectorLower(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_LESS);
}

void vectorLowerEqual(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_LESS_EQUAL);
}

void vectorEqual(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_EQUAL);
}

void vectorNotEqual(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_NOT_EQUAL);
}

void vectorIn(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_IN);
}

void vectorNotIn(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_NOT_IN);
}

void vectorLike(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_LIKE);
}

void vectorNotLike(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_NOT_LIKE);
}

void vectorMatch(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_MATCH);
}

void vectorNotMatch(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  vectorCompare(pLeft, pRight, out, _ord, TSDB_RELATION_NMATCH);
}

void vectorIsNull(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;
  bool res = false;
  
  bool *output=(bool *)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);

  for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
    if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type)) {
      res = true;
      SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
      continue;
    }

    res = false;
    SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
  }
}

void vectorNotNull(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->num, pRight->num) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;
  bool res = false;
  
  bool *output = (bool *)out;
  _getValueAddr_fn_t getVectorValueAddrFnLeft = getVectorValueAddrFn(pLeft->type);

  for (; i >= 0 && i < pLeft->num; i += step, output += 1) {
    if (isNull(getVectorValueAddrFnLeft(pLeft->data,i), pLeft->type)) {
      res = false;
      SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
      continue;
    }

    res = true;
    SET_TYPED_DATA(output, TSDB_DATA_TYPE_BOOL, res);
  }
}


_bin_scalar_fn_t getBinScalarOperatorFn(int32_t binFunctionId) {
  switch (binFunctionId) {
    case OP_TYPE_ADD:
      return vectorAdd;
    case OP_TYPE_SUB:
      return vectorSub;
    case OP_TYPE_MULTI:
      return vectorMultiply;
    case OP_TYPE_DIV:
      return vectorDivide;
    case OP_TYPE_MOD:
      return vectorRemainder;
    case OP_TYPE_GREATER_THAN:
      return vectorGreater;
    case OP_TYPE_GREATER_EQUAL:
      return vectorGreaterEqual;
    case OP_TYPE_LOWER_THAN:
      return vectorLower;
    case OP_TYPE_LOWER_EQUAL:
      return vectorLowerEqual;
    case OP_TYPE_EQUAL:
      return vectorEqual;
    case OP_TYPE_NOT_EQUAL:
      return vectorNotEqual;
    case OP_TYPE_IN:
      return vectorIn;
    case OP_TYPE_NOT_IN:
      return vectorNotIn;
    case OP_TYPE_LIKE:
      return vectorLike;
    case OP_TYPE_NOT_LIKE:
      return vectorNotLike;
    case OP_TYPE_MATCH:
      return vectorMatch;
    case OP_TYPE_NMATCH:
      return vectorNotMatch;
    case OP_TYPE_IS_NULL:
      return vectorIsNull;
    case OP_TYPE_IS_NOT_NULL:
      return vectorNotNull;
    case OP_TYPE_BIT_AND:
      return vectorBitAnd;
    case OP_TYPE_BIT_OR:
      return vectorBitOr;
    default:
      assert(0);
      return NULL;
  }
}

bool isBinaryStringOp(int32_t op) {
  return op == TSDB_BINARY_OP_CONCAT;
}
