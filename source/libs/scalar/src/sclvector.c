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

#include "filter.h"
#include "filterInt.h"
#include "query.h"
#include "querynodes.h"
#include "sclInt.h"
#include "sclvector.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttypes.h"

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
void* getVectorValueAddr_default(void *src, int32_t index) {
  return src;
}
void* getVectorValueAddr_VAR(void *src, int32_t index) {
  return colDataGetData((SColumnInfoData *)src, index);
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
    }else if(srcType==TSDB_DATA_TYPE_BINARY) {
        p = getVectorValueAddr_VAR;
    }else if(srcType==TSDB_DATA_TYPE_NCHAR) {
        p = getVectorValueAddr_VAR;
    }else {
        p = getVectorValueAddr_default;
    }
    return p;
}

static FORCE_INLINE void varToSigned(char *buf, SScalarParam* pOut, int32_t rowIndex) {
  int64_t value = strtoll(buf, NULL, 10);
  colDataAppendInt64(pOut->columnData, rowIndex, &value);
}

static FORCE_INLINE void varToUnsigned(char *buf, SScalarParam* pOut, int32_t rowIndex) {
  uint64_t value = strtoull(buf, NULL, 10);
  colDataAppendInt64(pOut->columnData, rowIndex, (int64_t*) &value);
}

static FORCE_INLINE void varToFloat(char *buf, SScalarParam* pOut, int32_t rowIndex) {
  double value = strtod(buf, NULL);
  colDataAppendDouble(pOut->columnData, rowIndex, &value);
}

static FORCE_INLINE void varToBool(char *buf, SScalarParam* pOut, int32_t rowIndex) {
  int64_t value = strtoll(buf, NULL, 10);
  bool v = (value != 0)? true:false;
  colDataAppendInt8(pOut->columnData, rowIndex, (int8_t*) &v);
}

static FORCE_INLINE void varToNchar(char* buf, SScalarParam* pOut, int32_t rowIndex) {
  int32_t len = 0;
  int32_t inputLen = varDataLen(buf);

  char* t = taosMemoryCalloc(1,(inputLen + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
  /*int32_t resLen = */taosMbsToUcs4(varDataVal(buf), inputLen, (TdUcs4*) varDataVal(t), pOut->columnData->info.bytes, &len);
  varDataSetLen(t, len);

  colDataAppend(pOut->columnData, rowIndex, t, false);
  taosMemoryFree(t);
}

//TODO opt performance, tmp is not needed.
int32_t vectorConvertFromVarData(const SScalarParam* pIn, SScalarParam* pOut, int32_t inType, int32_t outType) {
  int32_t bufSize = pIn->columnData->info.bytes;
  char *tmp = taosMemoryMalloc(bufSize + VARSTR_HEADER_SIZE);

  bool vton = false;

  _bufConverteFunc func = NULL;
  if (TSDB_DATA_TYPE_BOOL == outType) {
    func = varToBool;
  } else if (IS_SIGNED_NUMERIC_TYPE(outType) || TSDB_DATA_TYPE_TIMESTAMP == outType) {
    func = varToSigned;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(outType)) {
    func = varToUnsigned;
  } else if (IS_FLOAT_TYPE(outType)) {
    func = varToFloat;
  } else if (outType == TSDB_DATA_TYPE_NCHAR) {
    func = varToNchar;
    vton = true;
  } else {
    sclError("invalid convert outType:%d", outType);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  pOut->numOfRows = pIn->numOfRows;
  for (int32_t i = 0; i < pIn->numOfRows; ++i) {
    if (colDataIsNull_s(pIn->columnData, i)) {
      colDataAppendNULL(pOut->columnData, i);
      continue;
    }

    char* data = colDataGetData(pIn->columnData, i);
    if (vton) {
      memcpy(tmp, data, varDataTLen(data));
    } else {
      if (TSDB_DATA_TYPE_VARCHAR == inType) {
        memcpy(tmp, varDataVal(data), varDataLen(data));
        tmp[varDataLen(data)] = 0;
      } else {
        ASSERT(varDataLen(data) <= bufSize);

        int len = taosUcs4ToMbs((TdUcs4 *)varDataVal(data), varDataLen(data), tmp);
        if (len < 0) {
          sclError("castConvert taosUcs4ToMbs error 1");
          taosMemoryFreeClear(tmp);
          return TSDB_CODE_QRY_APP_ERROR;
        }

        tmp[len] = 0;
      }
    }
    
    (*func)(tmp, pOut, i);
  }
  
  taosMemoryFreeClear(tmp);
  return TSDB_CODE_SUCCESS;
}

// TODO opt performance
int32_t vectorConvertImpl(const SScalarParam* pIn, SScalarParam* pOut) {
  SColumnInfoData* pInputCol  = pIn->columnData;
  SColumnInfoData* pOutputCol = pOut->columnData;

  int16_t inType   = pInputCol->info.type;
  int16_t outType  = pOutputCol->info.type;

  if (IS_VAR_DATA_TYPE(inType)) {
    return vectorConvertFromVarData(pIn, pOut, inType, outType);
  }
  
  switch (outType) {
    case TSDB_DATA_TYPE_BOOL: {
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        bool value = 0;
        GET_TYPED_DATA(value, bool, inType, colDataGetData(pInputCol, i));
        colDataAppendInt8(pOutputCol, i, (int8_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        int8_t value = 0;
        GET_TYPED_DATA(value, int8_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt8(pOutputCol, i, (int8_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        int16_t value = 0;
        GET_TYPED_DATA(value, int16_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt16(pOutputCol, i, (int16_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        int32_t value = 0;
        GET_TYPED_DATA(value, int32_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt32(pOutputCol, i, (int32_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        int64_t value = 0;
        GET_TYPED_DATA(value, int64_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt64(pOutputCol, i, (int64_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        uint8_t value = 0;
        GET_TYPED_DATA(value, uint8_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt8(pOutputCol, i, (int8_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        uint16_t value = 0;
        GET_TYPED_DATA(value, uint16_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt16(pOutputCol, i, (int16_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        uint32_t value = 0;
        GET_TYPED_DATA(value, uint32_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt32(pOutputCol, i, (int32_t *)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        uint64_t value = 0;
        GET_TYPED_DATA(value, uint64_t, inType, colDataGetData(pInputCol, i));
        colDataAppendInt64(pOutputCol, i, (int64_t*)&value);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:{
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        float value = 0;
        GET_TYPED_DATA(value, float, inType, colDataGetData(pInputCol, i));
        colDataAppendFloat(pOutputCol, i, (float*)&value);
      }
      break;  
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      for (int32_t i = 0; i < pIn->numOfRows; ++i) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        
        double value = 0;
        GET_TYPED_DATA(value, double, inType, colDataGetData(pInputCol, i));
        colDataAppendDouble(pOutputCol, i, (double*)&value);
      }
      break;  
    }
    default:
      sclError("invalid convert output type:%d", outType);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int8_t gConvertTypes[TSDB_DATA_TYPE_BLOB+1][TSDB_DATA_TYPE_BLOB+1] = {
/*         NULL BOOL TINY SMAL INT  BIG  FLOA DOUB VARC TIME NCHA UTIN USMA UINT UBIG VARB JSON DECI BLOB */
/*NULL*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*BOOL*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   0,   12,  13,  14,  7,   0,   0,   0,
/*TINY*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   7,   0,   0,   0,
/*SMAL*/   0,   0,   0,   0,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   7,   0,   0,   0,
/*INT */   0,   0,   0,   0,   0,   5,   6,   7,   7,   9,   7,   4,   4,   5,   7,   7,   0,   0,   0,
/*BIGI*/   0,   0,   0,   0,   0,   0,   6,   7,   7,   0,   7,   5,   5,   5,   7,   7,   0,   0,   0,
/*FLOA*/   0,   0,   0,   0,   0,   0,   0,   7,   7,   6,   7,   6,   6,   6,   6,   7,   0,   0,   0,
/*DOUB*/   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   7,   7,   7,   7,   0,   0,   0,
/*VARC*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   0,   7,   7,   7,   7,   0,   0,   0,   0,
/*TIME*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   9,   9,   9,   7,   7,   0,   0,   0,
/*NCHA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   0,   0,   0,   0,
/*UTIN*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   12,  13,  14,  7,   0,   0,   0,
/*USMA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   13,  14,  7,   0,   0,   0,
/*UINT*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   14,  7,   0,   0,   0,
/*UBIG*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   0,   0,   0,
/*VARB*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*JSON*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*DECI*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*BLOB*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0
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
  if (pLeft->pHashFilter != NULL || pRight->pHashFilter != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t leftType  = GET_PARAM_TYPE(pLeft);
  int32_t rightType = GET_PARAM_TYPE(pRight);
  if (leftType == rightType) {
    return TSDB_CODE_SUCCESS;
  }

  SScalarParam *param1 = NULL, *paramOut1 = NULL; 
  SScalarParam *param2 = NULL, *paramOut2 = NULL;
  int32_t code = 0;
  
  if (leftType < rightType) {
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

  int8_t type = vectorGetConvertType(GET_PARAM_TYPE(param1), GET_PARAM_TYPE(param2));
  if (0 == type) {
    return TSDB_CODE_SUCCESS;
  }

  if (type != GET_PARAM_TYPE(param1)) {
    SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
    paramOut1->numOfRows = param1->numOfRows;

    paramOut1->columnData = createColumnInfoData(&t, param1->numOfRows);
    if (paramOut1->columnData == NULL) {
      return terrno;
    }

    code = vectorConvertImpl(param1, paramOut1);
    if (code) {
//      taosMemoryFreeClear(paramOut1->data);
      return code;
    }
  }
  
  if (type != GET_PARAM_TYPE(param2)) {
    SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
    paramOut2->numOfRows = param2->numOfRows;

    paramOut2->columnData = createColumnInfoData(&t, param2->numOfRows);
    if (paramOut2->columnData == NULL) {
      return terrno;
    }

    code = vectorConvertImpl(param2, paramOut2);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

enum {
  VECTOR_DO_CONVERT = 0x1,
  VECTOR_UN_CONVERT = 0x2,
};

static int32_t doConvertHelper(SScalarParam* pDest, int32_t* convert, const SScalarParam* pParam, int32_t type) {
  SColumnInfoData* pCol = pParam->columnData;

  if (IS_VAR_DATA_TYPE(pCol->info.type)) {
    pDest->numOfRows = pParam->numOfRows;

    SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
    pDest->columnData = createColumnInfoData(&t, pParam->numOfRows);
    if (pDest->columnData == NULL) {
      sclError("malloc %d failed", (int32_t)(pParam->numOfRows * sizeof(double)));
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t code = vectorConvertImpl(pParam, pDest);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    *convert = VECTOR_DO_CONVERT;
  } else {
    *convert = VECTOR_UN_CONVERT;
  }

  return TSDB_CODE_SUCCESS;
}

// TODO not correct for descending order scan
static void vectorMathAddHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) + getVectorDoubleValueFnRight(pRightCol->pData, 0);
    }
    pOutputCol->hasNull = pLeftCol->hasNull;
    if (pOutputCol->hasNull) {
      memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(numOfRows));
    }
  }
}

static SColumnInfoData* doVectorConvert(SScalarParam* pInput, int32_t* doConvert) {
  SScalarParam convertParam = {0};

  int32_t code = doConvertHelper(&convertParam, doConvert, pInput, TSDB_DATA_TYPE_DOUBLE);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  if (*doConvert == VECTOR_DO_CONVERT) {
    return convertParam.columnData;
  } else {
    return pInput->columnData;
  }
}

static void doReleaseVec(SColumnInfoData* pCol, int32_t type) {
  if (type == VECTOR_DO_CONVERT) {
    colDataDestroy(pCol);
  }
}

void vectorMathAdd(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol  = doVectorConvert(pRight, &rightConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) + getVectorDoubleValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] | pRightCol->nullbitmap[j];
      }
    }

  } else if (pLeft->numOfRows == 1) {
    vectorMathAddHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
  } else if (pRight->numOfRows == 1) {
    vectorMathAddHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
  }

  doReleaseVec(pLeftCol, leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

// TODO not correct for descending order scan
static void vectorMathSubHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t factor, int32_t i) {
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      *output = (getVectorDoubleValueFnLeft(pLeftCol->pData, i) - getVectorDoubleValueFnRight(pRightCol->pData, 0)) * factor;
    }
    pOutputCol->hasNull = pLeftCol->hasNull;
    if (pOutputCol->hasNull) {
      memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(numOfRows));
    }
  }
}

void vectorMathSub(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol  = doVectorConvert(pRight, &rightConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) - getVectorDoubleValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] | pRightCol->nullbitmap[j];
      }
    }

  } else if (pLeft->numOfRows == 1) {
    vectorMathSubHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, -1, i);
  } else if (pRight->numOfRows == 1) {
    vectorMathSubHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, 1, i);
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

// TODO not correct for descending order scan
static void vectorMathMultiplyHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) * getVectorDoubleValueFnRight(pRightCol->pData, 0);
    }
    pOutputCol->hasNull = pLeftCol->hasNull;
    if (pOutputCol->hasNull) {
      memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(numOfRows));
    }
  }
}

void vectorMathMultiply(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;
  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol  = doVectorConvert(pRight, &rightConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) * getVectorDoubleValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] | pRightCol->nullbitmap[j];
      }
    }

  } else if (pLeft->numOfRows == 1) {
    vectorMathMultiplyHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
  } else if (pRight->numOfRows == 1) {
    vectorMathMultiplyHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
  }

  doReleaseVec(pLeftCol, leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

void vectorMathDivide(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;
  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol  = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol = doVectorConvert(pRight, &rightConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {  // check for the 0 value
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) / getVectorDoubleValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] | pRightCol->nullbitmap[j];
      }
    }

  } else if (pLeft->numOfRows == 1) {
    if (colDataIsNull_f(pLeftCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pRight->numOfRows);
    } else {
      for (; i >= 0 && i < pRight->numOfRows; i += step, output += 1) {
        *output = getVectorDoubleValueFnLeft(pLeftCol->pData, 0) / getVectorDoubleValueFnRight(pRightCol->pData, i);
      }
      pOutputCol->hasNull = pRightCol->hasNull;
      if (pOutputCol->hasNull) {
        memcpy(pOutputCol->nullbitmap, pRightCol->nullbitmap, BitmapLen(pRight->numOfRows));
      }
    }
  } else if (pRight->numOfRows == 1) {
    if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pLeft->numOfRows);
    } else {
      for (; i >= 0 && i < pLeft->numOfRows; i += step, output += 1) {
        *output = getVectorDoubleValueFnLeft(pLeftCol->pData, i) / getVectorDoubleValueFnRight(pRightCol->pData, 0);
      }
      pOutputCol->hasNull = pLeftCol->hasNull;
      if (pOutputCol->hasNull) {
        memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(pLeft->numOfRows));
      }
    }
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

void vectorMathRemainder(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;
  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol  = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol = doVectorConvert(pRight, &rightConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;
  double zero = 0.0;

  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      if (colDataIsNull_f(pLeftCol->nullbitmap, i) || colDataIsNull_f(pRightCol->nullbitmap, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }

      double lx = getVectorDoubleValueFnLeft(pLeftCol->pData, i);
      double rx = getVectorDoubleValueFnRight(pRightCol->pData, i);
      if (isnan(lx) || isinf(lx) || isnan(rx) || isinf(rx)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }

      *output = lx - ((int64_t)(lx / rx)) * rx;
    }
  } else if (pLeft->numOfRows == 1) {
    double lx = getVectorDoubleValueFnLeft(pLeftCol->pData, 0);
    if (colDataIsNull_f(pLeftCol->nullbitmap, 0) || isnan(lx) || isinf(lx)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pRight->numOfRows);
    } else {
      for (; i >= 0 && i < pRight->numOfRows; i += step, output += 1) {
        if (colDataIsNull_f(pRightCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        double rx = getVectorDoubleValueFnRight(pRightCol->pData, i);
        if (isnan(rx) || isinf(rx) || FLT_EQUAL(rx, 0)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        *output = lx - ((int64_t)(lx / rx)) * rx;
      }
    }
  } else if (pRight->numOfRows == 1) {
    double rx = getVectorDoubleValueFnRight(pRightCol->pData, 0);
    if (colDataIsNull_f(pRightCol->nullbitmap, 0) || FLT_EQUAL(rx, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pLeft->numOfRows);
    } else {
      for (; i >= 0 && i < pLeft->numOfRows; i += step, output += 1) {
        if (colDataIsNull_f(pLeftCol->nullbitmap, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        double lx = getVectorDoubleValueFnLeft(pLeftCol->pData, i);
        if (isnan(lx) || isinf(lx)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        *output = lx - ((int64_t)(lx / rx)) * rx;
      }
    }
  }

  doReleaseVec(pLeftCol, leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

void vectorMathMinus(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  pOut->numOfRows = pLeft->numOfRows;

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : (pLeft->numOfRows - 1);
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  int32_t leftConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);

  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);

  double *output = (double *)pOutputCol->pData;
  for (; i < pLeft->numOfRows && i >= 0; i += step, output += 1) {
    *output = - getVectorDoubleValueFnLeft(pLeftCol->pData, i);
  }

  pOutputCol->hasNull = pLeftCol->hasNull;
  if (pOutputCol->hasNull) {
    memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(pLeft->numOfRows));
  }

  doReleaseVec(pLeftCol,  leftConvert);
}

void vectorConcat(SScalarParam* pLeft, SScalarParam* pRight, void *out, int32_t _ord) {
#if 0
  int32_t len = pLeft->bytes + pRight->bytes;

  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  char *output = (char *)out;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += len) {
      char* left = POINTER_SHIFT(pLeft->data, pLeft->bytes * i);
      char* right = POINTER_SHIFT(pRight->data, pRight->bytes * i);

      if (isNull(left, pLeftCol->info.type) || isNull(right, pRight->info.type)) {
        setVardataNull(output, TSDB_DATA_TYPE_BINARY);
        continue;
      }

      // todo define a macro
      memcpy(varDataVal(output), varDataVal(left), varDataLen(left));
      memcpy(varDataVal(output) + varDataLen(left), varDataVal(right), varDataLen(right));
      varDataSetLen(output, varDataLen(left) + varDataLen(right));
    }
  } else if (pLeft->numOfRows == 1) {
    for (; i >= 0 && i < pRight->numOfRows; i += step, output += len) {
      char *right = POINTER_SHIFT(pRight->data, pRight->bytes * i);
      if (isNull(pLeft->data, pLeftCol->info.type) || isNull(right, pRight->info.type)) {
        setVardataNull(output, TSDB_DATA_TYPE_BINARY);
        continue;
      }

      memcpy(varDataVal(output), varDataVal(pLeft->data), varDataLen(pLeft->data));
      memcpy(varDataVal(output) + varDataLen(pLeft->data), varDataVal(right), varDataLen(right));
      varDataSetLen(output, varDataLen(pLeft->data) + varDataLen(right));
    }
  } else if (pRight->numOfRows == 1) {
    for (; i >= 0 && i < pLeft->numOfRows; i += step, output += len) {
      char* left = POINTER_SHIFT(pLeft->data, pLeft->bytes * i);
      if (isNull(left, pLeftCol->info.type) || isNull(pRight->data, pRight->info.type)) {
        SET_DOUBLE_NULL(output);
        continue;
      }

      memcpy(varDataVal(output), varDataVal(left), varDataLen(pRight->data));
      memcpy(varDataVal(output) + varDataLen(left), varDataVal(pRight->data), varDataLen(pRight->data));
      varDataSetLen(output, varDataLen(left) + varDataLen(pRight->data));
    }
  }
#endif
}

static void vectorBitAndHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) & getVectorBigintValueFnRight(pRightCol->pData, 0);
    }
    pOutputCol->hasNull = pLeftCol->hasNull;
    if (pOutputCol->hasNull) {
      memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(numOfRows));
    }
  }
}

void vectorBitAnd(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;
  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol  = doVectorConvert(pRight, &rightConvert);

  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  int64_t *output = (int64_t *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) & getVectorBigintValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] & pRightCol->nullbitmap[j];
      }
    }

  } else if (pLeft->numOfRows == 1) {
    vectorBitAndHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
  } else if (pRight->numOfRows == 1) {
    vectorBitAndHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

static void vectorBitOrHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  int64_t *output = (int64_t *)pOutputCol->pData;

  if (colDataIsNull_f(pRightCol->nullbitmap, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    int64_t rx = getVectorBigintValueFnRight(pRightCol->pData, 0);
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) | rx;
    }
    pOutputCol->hasNull = pLeftCol->hasNull;
    if (pOutputCol->hasNull) {
      memcpy(pOutputCol->nullbitmap, pLeftCol->nullbitmap, BitmapLen(numOfRows));
    }
  }
}

void vectorBitOr(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;
  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  int32_t i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  int32_t leftConvert = 0, rightConvert = 0;
  SColumnInfoData *pLeftCol   = doVectorConvert(pLeft, &leftConvert);
  SColumnInfoData *pRightCol  = doVectorConvert(pRight, &rightConvert);

  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  int64_t *output = (int64_t *)pOutputCol->pData;
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) | getVectorBigintValueFnRight(pRightCol->pData, i);
    }

    pOutputCol->hasNull = (pLeftCol->hasNull || pRightCol->hasNull);
    if (pOutputCol->hasNull) {
      int32_t numOfBitLen = BitmapLen(pLeft->numOfRows);
      for (int32_t j = 0; j < numOfBitLen; ++j) {
        pOutputCol->nullbitmap[j] = pLeftCol->nullbitmap[j] | pRightCol->nullbitmap[j];
      }
    }
  } else if (pLeft->numOfRows == 1) {
    vectorBitOrHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
  } else if (pRight->numOfRows == 1) {
    vectorBitOrHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

void vectorCompareImpl(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord, int32_t optr) {
  int32_t       i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t       step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;
  __compar_fn_t fp = filterGetCompFunc(GET_PARAM_TYPE(pLeft), optr);

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  if (pRight->pHashFilter != NULL) {
    for (; i >= 0 && i < pLeft->numOfRows; i += step) {
      if (colDataIsNull_s(pLeft->columnData, i)) {
        continue;
      }

      char *pLeftData = colDataGetData(pLeft->columnData, i);
      bool  res = filterDoCompare(fp, optr, pLeftData, pRight->pHashFilter);
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
    }
    return;
  }

  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step) {
      if (colDataIsNull_s(pLeft->columnData, i) || colDataIsNull_s(pRight->columnData, i)) {
        continue;  // TODO set null or ignore
      }

      char *pLeftData = colDataGetData(pLeft->columnData, i);
      char *pRightData = colDataGetData(pRight->columnData, i);
      bool  res = filterDoCompare(fp, optr, pLeftData, pRightData);
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
    }
  } else if (pRight->numOfRows == 1) {
    char *pRightData = colDataGetData(pRight->columnData, 0);
    ASSERT(pLeft->pHashFilter == NULL);

    for (; i >= 0 && i < pLeft->numOfRows; i += step) {
      if (colDataIsNull_s(pLeft->columnData, i)) {
        continue;
      }

      char *pLeftData = colDataGetData(pLeft->columnData, i);
      bool  res = filterDoCompare(fp, optr, pLeftData, pRightData);
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
    }
  } else if (pLeft->numOfRows == 1) {
    char *pLeftData = colDataGetData(pLeft->columnData, 0);
    for (; i >= 0 && i < pRight->numOfRows; i += step) {
      if (colDataIsNull_s(pRight->columnData, i)) {
        continue;
      }

      char *pRightData = colDataGetData(pLeft->columnData, i);
      bool  res = filterDoCompare(fp, optr, pLeftData, pRightData);
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
    }
  }
}

void vectorCompare(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord, int32_t optr) {
  SScalarParam pLeftOut = {0}; 
  SScalarParam pRightOut = {0};
  
  vectorConvert(pLeft, pRight, &pLeftOut, &pRightOut);

  SScalarParam *param1 = NULL; 
  SScalarParam *param2 = NULL;

  if (pLeftOut.columnData != NULL) {
    param1 = &pLeftOut;
  } else {
    param1 = pLeft;
  }

  if (pRightOut.columnData != NULL) {
    param2 = &pRightOut;
  } else {
    param2 = pRight;
  }

  vectorCompareImpl(param1, param2, pOut, _ord, optr);
  sclFreeParam(&pLeftOut);
  sclFreeParam(&pRightOut);  
}

void vectorGreater(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_GREATER_THAN);
}

void vectorGreaterEqual(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_GREATER_EQUAL);
}

void vectorLower(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_LOWER_THAN);
}

void vectorLowerEqual(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_LOWER_EQUAL);
}

void vectorEqual(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_EQUAL);
}

void vectorNotEqual(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_NOT_EQUAL);
}

void vectorIn(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_IN);
}

void vectorNotIn(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_NOT_IN);
}

void vectorLike(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_LIKE);
}

void vectorNotLike(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_NOT_LIKE);
}

void vectorMatch(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_MATCH);
}

void vectorNotMatch(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorCompare(pLeft, pRight, pOut, _ord, OP_TYPE_NMATCH);
}

void vectorIsNull(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  for(int32_t i = 0; i < pLeft->numOfRows; ++i) {
    int8_t v = colDataIsNull_s(pLeft->columnData, i)? 1:0;
    colDataAppendInt8(pOut->columnData, i, &v);
  }
  pOut->numOfRows = pLeft->numOfRows;
}

void vectorNotNull(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  for(int32_t i = 0; i < pLeft->numOfRows; ++i) {
    int8_t v = colDataIsNull_s(pLeft->columnData, i)? 0:1;
    colDataAppendInt8(pOut->columnData, i, &v);
  }
  pOut->numOfRows = pLeft->numOfRows;
}

void vectorIsTrue(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorConvertImpl(pLeft, pOut);
}

_bin_scalar_fn_t getBinScalarOperatorFn(int32_t binFunctionId) {
  switch (binFunctionId) {
    case OP_TYPE_ADD:
      return vectorMathAdd;
    case OP_TYPE_SUB:
      return vectorMathSub;
    case OP_TYPE_MULTI:
      return vectorMathMultiply;
    case OP_TYPE_DIV:
      return vectorMathDivide;
    case OP_TYPE_MOD:
      return vectorMathRemainder;
    case OP_TYPE_MINUS:
      return vectorMathMinus;
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
    case OP_TYPE_IS_TRUE:
      return vectorIsTrue;
    default:
      assert(0);
      return NULL;
  }
}
