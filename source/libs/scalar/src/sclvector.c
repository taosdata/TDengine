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
#include "tdataformat.h"
#include "ttypes.h"
#include "ttime.h"

#define LEFT_COL ((pLeftCol->info.type == TSDB_DATA_TYPE_JSON ? (void*)pLeftCol : pLeftCol->pData))
#define RIGHT_COL ((pRightCol->info.type == TSDB_DATA_TYPE_JSON ? (void*)pRightCol : pRightCol->pData))

#define IS_NULL colDataIsNull_s(pLeft->columnData, i) || colDataIsNull_s(pRight->columnData, i) \
    || IS_JSON_NULL(pLeft->columnData->info.type, colDataGetVarData(pLeft->columnData, i)) \
    || IS_JSON_NULL(pRight->columnData->info.type, colDataGetVarData(pRight->columnData, i))

#define IS_HELPER_NULL(col,i) colDataIsNull_s(col, i) || IS_JSON_NULL(col->info.type, colDataGetVarData(col, i))

void convertNumberToNumber(const void *inData, void *outData, int8_t inType, int8_t outType){
  switch (outType) {
    case TSDB_DATA_TYPE_BOOL: {
      GET_TYPED_DATA(*((bool *)outData), bool, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      GET_TYPED_DATA(*((int8_t *)outData), int8_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      GET_TYPED_DATA(*((int16_t *)outData), int16_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      GET_TYPED_DATA(*((int32_t *)outData), int32_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      GET_TYPED_DATA(*((int64_t *)outData), int64_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      GET_TYPED_DATA(*((uint8_t *)outData), uint8_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      GET_TYPED_DATA(*((uint16_t *)outData), uint16_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      GET_TYPED_DATA(*((uint32_t *)outData), uint32_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      GET_TYPED_DATA(*((uint64_t *)outData), uint64_t, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      GET_TYPED_DATA(*((float *)outData), float, inType, inData);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      GET_TYPED_DATA(*((double *)outData), double, inType, inData);
      break;
    }
    default:{
      ASSERT(0);
    }
  }
}

void convertNcharToDouble(const void *inData, void *outData){
  char *tmp = taosMemoryMalloc(varDataTLen(inData));
  int len = taosUcs4ToMbs((TdUcs4 *)varDataVal(inData), varDataLen(inData), tmp);
  if (len < 0) {
    sclError("castConvert taosUcs4ToMbs error 1");
  }

  tmp[len] = 0;

  double value = taosStr2Double(tmp, NULL);

  *((double *)outData) = value;
  taosMemoryFreeClear(tmp);
}

void convertBinaryToDouble(const void *inData, void *outData){
  char *tmp = taosMemoryCalloc(1, varDataTLen(inData));
  if(tmp == NULL){
    *((double *)outData) = 0.;
    return;
  }
  memcpy(tmp, varDataVal(inData), varDataLen(inData));
  double ret = taosStr2Double(tmp, NULL);
  taosMemoryFree(tmp);
  *((double *)outData) = ret;
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
int64_t getVectorBigintValue_BOOL(void *src, int32_t index) {
  return (int64_t)*((bool *)src + index);
}

int64_t getVectorBigintValue_JSON(void *src, int32_t index){
  ASSERT(!colDataIsNull_var(((SColumnInfoData*)src), index));
  char *data = colDataGetVarData((SColumnInfoData*)src, index);
  double out = 0;
  if (*data == TSDB_DATA_TYPE_NULL){
    return 0;
  } else if(*data == TSDB_DATA_TYPE_NCHAR) {   // json inner type can not be BINARY
    convertNcharToDouble(data+CHAR_BYTES, &out);
  } else if(tTagIsJson(data)){
    terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
    return 0;
  } else {
    convertNumberToNumber(data+CHAR_BYTES, &out, *data, TSDB_DATA_TYPE_DOUBLE);
  }
  return (int64_t)out;
}

_getBigintValue_fn_t getVectorBigintValueFn(int32_t srcType) {
    _getBigintValue_fn_t p = NULL;
    if (srcType==TSDB_DATA_TYPE_TINYINT) {
      p = getVectorBigintValue_TINYINT;
    } else if (srcType==TSDB_DATA_TYPE_UTINYINT) {
      p = getVectorBigintValue_UTINYINT;
    } else if (srcType==TSDB_DATA_TYPE_SMALLINT) {
      p = getVectorBigintValue_SMALLINT;
    } else if (srcType==TSDB_DATA_TYPE_USMALLINT) {
      p = getVectorBigintValue_USMALLINT;
    } else if (srcType==TSDB_DATA_TYPE_INT) {
      p = getVectorBigintValue_INT;
    } else if (srcType==TSDB_DATA_TYPE_UINT) {
      p = getVectorBigintValue_UINT;
    } else if (srcType==TSDB_DATA_TYPE_BIGINT) {
      p = getVectorBigintValue_BIGINT;
    } else if (srcType==TSDB_DATA_TYPE_UBIGINT) {
      p = getVectorBigintValue_UBIGINT;
    } else if (srcType==TSDB_DATA_TYPE_FLOAT) {
      p = getVectorBigintValue_FLOAT;
    } else if (srcType==TSDB_DATA_TYPE_DOUBLE) {
      p = getVectorBigintValue_DOUBLE;
    } else if (srcType==TSDB_DATA_TYPE_TIMESTAMP) {
      p = getVectorBigintValue_BIGINT;
    } else if (srcType==TSDB_DATA_TYPE_BOOL) {
      p = getVectorBigintValue_BOOL;
    } else if (srcType==TSDB_DATA_TYPE_JSON) {
      p = getVectorBigintValue_JSON;
    } else if (srcType==TSDB_DATA_TYPE_NULL){
      p = NULL;
    } else {
      ASSERT(0);
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

static FORCE_INLINE void varToTimestamp(char *buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  int64_t value = 0;
  if (taosParseTime(buf, &value, strlen(buf), pOut->columnData->info.precision, tsDaylight) != TSDB_CODE_SUCCESS) {
    value = 0;
  }
  
  colDataAppendInt64(pOut->columnData, rowIndex, &value);
}

static FORCE_INLINE void varToSigned(char *buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  if (overflow) {
    int64_t minValue = tDataTypes[pOut->columnData->info.type].minValue;
    int64_t maxValue = tDataTypes[pOut->columnData->info.type].maxValue;
    int64_t value = (int64_t)taosStr2Int64(buf, NULL, 10);
    if (value > maxValue) {
      *overflow = 1;
      return;
    } else if (value < minValue) {
      *overflow = -1;
      return;
    } else {
      *overflow = 0;
    }
  }
      
  switch (pOut->columnData->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t value = (int8_t)taosStr2Int8(buf, NULL, 10);
     
      colDataAppendInt8(pOut->columnData, rowIndex, (int8_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t value = (int16_t)taosStr2Int16(buf, NULL, 10);
      colDataAppendInt16(pOut->columnData, rowIndex, (int16_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_INT: {
      int32_t value = (int32_t)taosStr2Int32(buf, NULL, 10);
      colDataAppendInt32(pOut->columnData, rowIndex, (int32_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t value = (int64_t)taosStr2Int64(buf, NULL, 10);
      colDataAppendInt64(pOut->columnData, rowIndex, (int64_t*)&value);
      break;
    }   
  }
}

static FORCE_INLINE void varToUnsigned(char *buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  if (overflow) {
    uint64_t minValue = (uint64_t)tDataTypes[pOut->columnData->info.type].minValue;
    uint64_t maxValue = (uint64_t)tDataTypes[pOut->columnData->info.type].maxValue;
    uint64_t value = (uint64_t)taosStr2UInt64(buf, NULL, 10);
    if (value > maxValue) {
      *overflow = 1;
      return;
    } else if (value < minValue) {
      *overflow = -1;
      return;
    } else {
      *overflow = 0;
    }
  }

  switch (pOut->columnData->info.type) {
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t value = (uint8_t)taosStr2UInt8(buf, NULL, 10);
      colDataAppendInt8(pOut->columnData, rowIndex, (int8_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t value = (uint16_t)taosStr2UInt16(buf, NULL, 10);
      colDataAppendInt16(pOut->columnData, rowIndex, (int16_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_UINT: {
      uint32_t value = (uint32_t)taosStr2UInt32(buf, NULL, 10);
      colDataAppendInt32(pOut->columnData, rowIndex, (int32_t*)&value);
      break;
    } 
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t value = (uint64_t)taosStr2UInt64(buf, NULL, 10);
      colDataAppendInt64(pOut->columnData, rowIndex, (int64_t*)&value);
      break;
    }   
  }
}

static FORCE_INLINE void varToFloat(char *buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  if (TSDB_DATA_TYPE_FLOAT == pOut->columnData->info.type) {
    float value = taosStr2Float(buf, NULL);
    colDataAppendFloat(pOut->columnData, rowIndex, &value);
    return;
  }
  
  double value = taosStr2Double(buf, NULL);
  colDataAppendDouble(pOut->columnData, rowIndex, &value);
}

static FORCE_INLINE void varToBool(char *buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  int64_t value = taosStr2Int64(buf, NULL, 10);
  bool v = (value != 0)? true:false;
  colDataAppendInt8(pOut->columnData, rowIndex, (int8_t*) &v);
}

static FORCE_INLINE void varToNchar(char* buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  int32_t len = 0;
  int32_t inputLen = varDataLen(buf);
  int32_t outputMaxLen = (inputLen + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;

  char* t = taosMemoryCalloc(1, outputMaxLen);
  /*int32_t resLen = */taosMbsToUcs4(varDataVal(buf), inputLen, (TdUcs4*) varDataVal(t), outputMaxLen - VARSTR_HEADER_SIZE, &len);
  varDataSetLen(t, len);

  colDataAppend(pOut->columnData, rowIndex, t, false);
  taosMemoryFree(t);
}

static FORCE_INLINE void ncharToVar(char* buf, SScalarParam* pOut, int32_t rowIndex, int32_t* overflow) {
  int32_t inputLen = varDataLen(buf);

  char* t = taosMemoryCalloc(1, inputLen + VARSTR_HEADER_SIZE);
  int32_t len  = taosUcs4ToMbs((TdUcs4 *)varDataVal(buf), varDataLen(buf), varDataVal(t));
  if (len < 0) {
    taosMemoryFree(t);
    return;
  }
  varDataSetLen(t, len);

  colDataAppend(pOut->columnData, rowIndex, t, false);
  taosMemoryFree(t);
}


//TODO opt performance, tmp is not needed.
int32_t vectorConvertFromVarData(const SScalarParam* pIn, SScalarParam* pOut, int32_t inType, int32_t outType, int32_t* overflow) {
  bool vton = false;

  _bufConverteFunc func = NULL;
  if (TSDB_DATA_TYPE_BOOL == outType) {
    func = varToBool;
  } else if (IS_SIGNED_NUMERIC_TYPE(outType)) {
    func = varToSigned;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(outType)) {
    func = varToUnsigned;
  } else if (IS_FLOAT_TYPE(outType)) {
    func = varToFloat;
  } else if (outType == TSDB_DATA_TYPE_BINARY) {   // nchar -> binary
    ASSERT(inType == TSDB_DATA_TYPE_NCHAR);
    func = ncharToVar;
    vton = true;
  } else if (outType == TSDB_DATA_TYPE_NCHAR) {   // binary -> nchar
    ASSERT(inType == TSDB_DATA_TYPE_VARCHAR);
    func = varToNchar;
    vton = true;
  } else if (TSDB_DATA_TYPE_TIMESTAMP == outType) {
    func = varToTimestamp;
  } else {
    sclError("invalid convert outType:%d", outType);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  pOut->numOfRows = pIn->numOfRows;
  for (int32_t i = 0; i < pIn->numOfRows; ++i) {
    if (IS_HELPER_NULL(pIn->columnData, i)) {
      colDataAppendNULL(pOut->columnData, i);
      continue;
    }

    char* data = colDataGetVarData(pIn->columnData, i);
    int32_t convertType = inType;
    if(inType == TSDB_DATA_TYPE_JSON){
      if(*data == TSDB_DATA_TYPE_NULL) {
        ASSERT(0);
      } else if(*data == TSDB_DATA_TYPE_NCHAR) {
        data += CHAR_BYTES;
        convertType = TSDB_DATA_TYPE_NCHAR;
      } else if(tTagIsJson(data)){
        terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
        return terrno;
      } else {
        convertNumberToNumber(data+CHAR_BYTES, colDataGetNumData(pOut->columnData, i), *data, outType);
        continue;
      }
    }
    int32_t bufSize = pIn->columnData->info.bytes;
    char *tmp = taosMemoryMalloc(varDataTLen(data));
    if(!tmp){
      sclError("out of memory in vectorConvertFromVarData");
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (vton) {
      memcpy(tmp, data, varDataTLen(data));
    } else {
      if (TSDB_DATA_TYPE_VARCHAR == convertType) {
        memcpy(tmp, varDataVal(data), varDataLen(data));
        tmp[varDataLen(data)] = 0;
      } else if (TSDB_DATA_TYPE_NCHAR == convertType){
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
    
    (*func)(tmp, pOut, i, overflow);
    taosMemoryFreeClear(tmp);
  }
  
  return TSDB_CODE_SUCCESS;
}

double getVectorDoubleValue_JSON(void *src, int32_t index){
  char *data = colDataGetVarData((SColumnInfoData*)src, index);
  double out = 0;
  if (*data == TSDB_DATA_TYPE_NULL){
    return out;
  } else if(*data == TSDB_DATA_TYPE_NCHAR) {   // json inner type can not be BINARY
    convertNcharToDouble(data+CHAR_BYTES, &out);
  } else if(tTagIsJson(data)){
    terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
    return 0;
  } else{
    convertNumberToNumber(data+CHAR_BYTES, &out, *data, TSDB_DATA_TYPE_DOUBLE);
  }
  return out;
}

void* ncharTobinary(void *buf){            // todo need to remove , if tobinary is nchar
  int32_t inputLen = varDataTLen(buf);

  void* t = taosMemoryCalloc(1, inputLen);
  int32_t len  = taosUcs4ToMbs((TdUcs4 *)varDataVal(buf), varDataLen(buf), varDataVal(t));
  if (len < 0) {
    sclError("charset:%s to %s. val:%s convert ncharTobinary failed.", DEFAULT_UNICODE_ENCODEC, tsCharset,
             (char*)varDataVal(buf));
    taosMemoryFree(t);
    return NULL;
  }
  varDataSetLen(t, len);
  return t;
}

bool convertJsonValue(__compar_fn_t *fp, int32_t optr, int8_t typeLeft, int8_t typeRight, char **pLeftData, char **pRightData,
                      void *pLeftOut, void *pRightOut, bool *isNull, bool *freeLeft, bool *freeRight){
  if(optr == OP_TYPE_JSON_CONTAINS) {
    return true;
  }

  if(typeLeft != TSDB_DATA_TYPE_JSON && typeRight != TSDB_DATA_TYPE_JSON){
    return true;
  }

  if(typeLeft == TSDB_DATA_TYPE_JSON){
    if(tTagIsJson(*pLeftData)){
      terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
      return false;
    }
    typeLeft = **pLeftData;
    (*pLeftData) ++;
  }
  if(typeRight == TSDB_DATA_TYPE_JSON){
    if(tTagIsJson(*pLeftData)){
      terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
      return false;
    }
    typeRight = **pRightData;
    (*pRightData) ++;
  }

  if(optr == OP_TYPE_LIKE || optr == OP_TYPE_NOT_LIKE || optr == OP_TYPE_MATCH || optr == OP_TYPE_NMATCH){
    if(typeLeft != TSDB_DATA_TYPE_NCHAR && typeLeft != TSDB_DATA_TYPE_BINARY){
      return false;
    }
  }

  // if types can not comparable
  if((IS_NUMERIC_TYPE(typeLeft) && !IS_NUMERIC_TYPE(typeRight)) ||
     (IS_NUMERIC_TYPE(typeRight) && !IS_NUMERIC_TYPE(typeLeft)) ||
     (IS_VAR_DATA_TYPE(typeLeft) && !IS_VAR_DATA_TYPE(typeRight)) ||
     (IS_VAR_DATA_TYPE(typeRight) && !IS_VAR_DATA_TYPE(typeLeft)) ||
     ((typeLeft == TSDB_DATA_TYPE_BOOL) && (typeRight != TSDB_DATA_TYPE_BOOL)) ||
     ((typeRight == TSDB_DATA_TYPE_BOOL) && (typeLeft != TSDB_DATA_TYPE_BOOL)))
    return false;


  if(typeLeft == TSDB_DATA_TYPE_NULL || typeRight == TSDB_DATA_TYPE_NULL){
    *isNull = true;
    return true;
  }
  int8_t type = vectorGetConvertType(typeLeft, typeRight);

  if(type == 0) {
    *fp = filterGetCompFunc(typeLeft, optr);
    return true;
  }

  *fp = filterGetCompFunc(type, optr);

  if(IS_NUMERIC_TYPE(type)){
    if(typeLeft == TSDB_DATA_TYPE_NCHAR) {
      ASSERT(0);
//      convertNcharToDouble(*pLeftData, pLeftOut);
//      *pLeftData = pLeftOut;
    } else if(typeLeft == TSDB_DATA_TYPE_BINARY) {
      ASSERT(0);
//      convertBinaryToDouble(*pLeftData, pLeftOut);
//      *pLeftData = pLeftOut;
    } else if(typeLeft != type) {
      convertNumberToNumber(*pLeftData, pLeftOut, typeLeft, type);
      *pLeftData = pLeftOut;
    }

    if(typeRight == TSDB_DATA_TYPE_NCHAR) {
      ASSERT(0);
//      convertNcharToDouble(*pRightData, pRightOut);
//      *pRightData = pRightOut;
    } else if(typeRight == TSDB_DATA_TYPE_BINARY) {
      ASSERT(0);
//      convertBinaryToDouble(*pRightData, pRightOut);
//      *pRightData = pRightOut;
    } else if(typeRight != type) {
      convertNumberToNumber(*pRightData, pRightOut, typeRight, type);
      *pRightData = pRightOut;
    }
  }else if(type == TSDB_DATA_TYPE_BINARY){
    if(typeLeft == TSDB_DATA_TYPE_NCHAR){
      *pLeftData = ncharTobinary(*pLeftData);
      *freeLeft = true;
    }
    if(typeRight == TSDB_DATA_TYPE_NCHAR){
      *pRightData = ncharTobinary(*pRightData);
      *freeRight = true;
    }
  }else{
    ASSERT(0);
  }

  return true;
}

int32_t vectorConvertToVarData(const SScalarParam* pIn, SScalarParam* pOut, int16_t inType, int16_t outType) {
  SColumnInfoData* pInputCol  = pIn->columnData;
  SColumnInfoData* pOutputCol = pOut->columnData;
  char tmp[128] = {0};

  if (IS_SIGNED_NUMERIC_TYPE(inType) || inType == TSDB_DATA_TYPE_BOOL || inType == TSDB_DATA_TYPE_TIMESTAMP) {
    for (int32_t i = 0; i < pIn->numOfRows; ++i) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }
      
      int64_t value = 0;
      GET_TYPED_DATA(value, int64_t, inType, colDataGetData(pInputCol, i));
      int32_t len = sprintf(varDataVal(tmp), "%" PRId64, value);
      varDataLen(tmp) = len;
      if (outType == TSDB_DATA_TYPE_NCHAR) {
        varToNchar(tmp, pOut, i, NULL);
      } else {
        colDataAppend(pOutputCol, i, (char *)tmp, false);
      }
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(inType)) {
    for (int32_t i = 0; i < pIn->numOfRows; ++i) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }
      
      uint64_t value = 0;
      GET_TYPED_DATA(value, uint64_t, inType, colDataGetData(pInputCol, i));
      int32_t len = sprintf(varDataVal(tmp), "%" PRIu64, value);
      varDataLen(tmp) = len;
      if (outType == TSDB_DATA_TYPE_NCHAR) {
        varToNchar(tmp, pOut, i, NULL);
      } else {
        colDataAppend(pOutputCol, i, (char *)tmp, false);
      }
    }
  } else if (IS_FLOAT_TYPE(inType)) {
    for (int32_t i = 0; i < pIn->numOfRows; ++i) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }
      
      double value = 0;
      GET_TYPED_DATA(value, double, inType, colDataGetData(pInputCol, i));
      int32_t len = sprintf(varDataVal(tmp), "%lf", value);
      varDataLen(tmp) = len;
      if (outType == TSDB_DATA_TYPE_NCHAR) {
        varToNchar(tmp, pOut, i, NULL);
      } else {
        colDataAppend(pOutputCol, i, (char *)tmp, false);
      }
    }
  } else {
    sclError("not supported input type:%d", inType);
    return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

// TODO opt performance
int32_t vectorConvertImpl(const SScalarParam* pIn, SScalarParam* pOut, int32_t* overflow) {
  SColumnInfoData* pInputCol  = pIn->columnData;
  SColumnInfoData* pOutputCol = pOut->columnData;

  if (NULL == pInputCol) {
    sclError("input column is NULL, hashFilter %p", pIn->pHashFilter);
    return TSDB_CODE_APP_ERROR;
  }
  
  int16_t inType   = pInputCol->info.type;
  int16_t outType  = pOutputCol->info.type;

  if (IS_VAR_DATA_TYPE(inType)) {
    return vectorConvertFromVarData(pIn, pOut, inType, outType, overflow);
  }

  if (overflow) {
    ASSERT(1 == pIn->numOfRows);

    pOut->numOfRows = 0;
    
    if (IS_SIGNED_NUMERIC_TYPE(outType)) {
      int64_t minValue = tDataTypes[outType].minValue;
      int64_t maxValue = tDataTypes[outType].maxValue;
      
      double value = 0;
      GET_TYPED_DATA(value, double, inType, colDataGetData(pInputCol, 0));
      
      if (value > maxValue) {
        *overflow = 1;
        return TSDB_CODE_SUCCESS;
      } else if (value < minValue) {
        *overflow = -1;
        return TSDB_CODE_SUCCESS;
      } else {
        *overflow = 0;
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(outType)) {
      uint64_t minValue = (uint64_t)tDataTypes[outType].minValue;
      uint64_t maxValue = (uint64_t)tDataTypes[outType].maxValue;
      
      double value = 0;
      GET_TYPED_DATA(value, double, inType, colDataGetData(pInputCol, 0));
      
      if (value > maxValue) {
        *overflow = 1;
        return TSDB_CODE_SUCCESS;
      } else if (value < minValue) {
        *overflow = -1;
        return TSDB_CODE_SUCCESS;
      } else {
        *overflow = 0;
      }
    }
  }

  pOut->numOfRows = pIn->numOfRows;
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
    case TSDB_DATA_TYPE_BINARY: 
    case TSDB_DATA_TYPE_NCHAR: {
      return vectorConvertToVarData(pIn, pOut, inType, outType);
    }
    default:
      sclError("invalid convert output type:%d", outType);
      return TSDB_CODE_QRY_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}



int8_t gConvertTypes[TSDB_DATA_TYPE_BLOB+1][TSDB_DATA_TYPE_BLOB+1] = {
/*         NULL BOOL TINY SMAL INT  BIG  FLOA DOUB VARC TIME NCHA UTIN USMA UINT UBIG JSON VARB DECI BLOB */
/*NULL*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*BOOL*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   0,   12,  13,  14,  0,   7,   0,   0,
/*TINY*/   0,   0,   0,   3,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   0,   7,   0,   0,
/*SMAL*/   0,   0,   0,   0,   4,   5,   6,   7,   7,   9,   7,   3,   4,   5,   7,   0,   7,   0,   0,
/*INT */   0,   0,   0,   0,   0,   5,   6,   7,   7,   9,   7,   4,   4,   5,   7,   0,   7,   0,   0,
/*BIGI*/   0,   0,   0,   0,   0,   0,   6,   7,   7,   0,   7,   5,   5,   5,   7,   0,   7,   0,   0,
/*FLOA*/   0,   0,   0,   0,   0,   0,   0,   7,   7,   6,   7,   6,   6,   6,   6,   0,   7,   0,   0,
/*DOUB*/   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   7,   7,   7,   0,   7,   0,   0,
/*VARC*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   9,   8,   7,   7,   7,   7,   0,   0,   0,   0,
/*TIME*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   9,   9,   9,   9,   7,   0,   7,   0,   0,
/*NCHA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   7,   7,   7,   0,   0,   0,   0,
/*UTIN*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   12,  13,  14,  0,   7,   0,   0,
/*USMA*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   13,  14,  0,   7,   0,   0,
/*UINT*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   14,  0,   7,   0,   0,
/*UBIG*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   7,   0,   0,
/*JSON*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
/*VARB*/   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
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

int32_t vectorConvertScalarParam(SScalarParam *input, SScalarParam *output, int32_t type) {
  SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
  output->numOfRows = input->numOfRows;

  int32_t code = sclCreateColumnInfoData(&t, input->numOfRows, output);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = vectorConvertImpl(input, output, NULL);
  if (code) {
//      taosMemoryFreeClear(paramOut1->data);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t vectorConvert(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam* pLeftOut, SScalarParam* pRightOut) {
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
    code = vectorConvertScalarParam(param1, paramOut1, type);
    if (code) {
      return code;
    }
  }
  
  if (type != GET_PARAM_TYPE(param2)) {
    code = vectorConvertScalarParam(param2, paramOut2, type);
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

  if (IS_VAR_DATA_TYPE(pCol->info.type) && pCol->info.type != TSDB_DATA_TYPE_JSON) {
    pDest->numOfRows = pParam->numOfRows;

    SDataType t = {.type = type, .bytes = tDataTypes[type].bytes};
    int32_t code = sclCreateColumnInfoData(&t, pParam->numOfRows, pDest);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = vectorConvertImpl(pParam, pDest, NULL);
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

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorDoubleValueFnLeft(LEFT_COL, i)
                + getVectorDoubleValueFnRight(RIGHT_COL, 0);
    }
  }
}

static void vectorMathTsAddHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  int64_t *output = (int64_t *)pOutputCol->pData;

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = taosTimeAdd(getVectorBigintValueFnLeft(pLeftCol->pData, i), getVectorBigintValueFnRight(pRightCol->pData, 0),
                            pRightCol->info.scale, pRightCol->info.precision);
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
    taosMemoryFree(pCol);
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

  if ((GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_TIMESTAMP && IS_INTEGER_TYPE(GET_PARAM_TYPE(pRight))) ||
      (GET_PARAM_TYPE(pRight) == TSDB_DATA_TYPE_TIMESTAMP && IS_INTEGER_TYPE(GET_PARAM_TYPE(pLeft))) ||
      (GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_TIMESTAMP && GET_PARAM_TYPE(pRight) == TSDB_DATA_TYPE_BOOL) ||
      (GET_PARAM_TYPE(pRight) == TSDB_DATA_TYPE_TIMESTAMP && GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_BOOL)) { //timestamp plus duration
    int64_t *output = (int64_t *)pOutputCol->pData;
    _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
    _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

    if (pLeft->numOfRows == 1 && pRight->numOfRows == 1) {
      if (GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_TIMESTAMP) {
        vectorMathTsAddHelper(pLeftCol, pRightCol, pOutputCol, pRight->numOfRows, step, i);
      } else {
        vectorMathTsAddHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
      }
    } else if (pLeft->numOfRows == 1) {
      vectorMathTsAddHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
    } else if (pRight->numOfRows == 1) {
      vectorMathTsAddHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
    } else if (pLeft->numOfRows == pRight->numOfRows) {
      for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
        if (IS_NULL) {
          colDataAppendNULL(pOutputCol, i);
          continue;  // TODO set null or ignore
        }
        *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) + getVectorBigintValueFnRight(pRightCol->pData, i);
      }
    } 
  } else {
    double *output = (double *)pOutputCol->pData;
    _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
    _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

    if (pLeft->numOfRows == pRight->numOfRows) {
      for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
        if (IS_NULL){
          colDataAppendNULL(pOutputCol, i);
          continue;  // TODO set null or ignore
        }
        *output = getVectorDoubleValueFnLeft(LEFT_COL, i) + getVectorDoubleValueFnRight(RIGHT_COL, i);
      }
    } else if (pLeft->numOfRows == 1) {
      vectorMathAddHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
    } else if (pRight->numOfRows == 1) {
      vectorMathAddHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
    }
  }

  doReleaseVec(pLeftCol, leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

// TODO not correct for descending order scan
static void vectorMathSubHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t factor, int32_t i) {
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = (getVectorDoubleValueFnLeft(LEFT_COL, i)
                 - getVectorDoubleValueFnRight(RIGHT_COL, 0)) * factor;
    }
  }
}

static void vectorMathTsSubHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t factor, int32_t i) {
  _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
  _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

  int64_t *output = (int64_t *)pOutputCol->pData;

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = taosTimeAdd(getVectorBigintValueFnLeft(pLeftCol->pData, i), -getVectorBigintValueFnRight(pRightCol->pData, 0),
                            pRightCol->info.scale, pRightCol->info.precision);
      
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

  if ((GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_TIMESTAMP && GET_PARAM_TYPE(pRight) == TSDB_DATA_TYPE_BIGINT) ||
      (GET_PARAM_TYPE(pRight) == TSDB_DATA_TYPE_TIMESTAMP && GET_PARAM_TYPE(pLeft) == TSDB_DATA_TYPE_BIGINT)) { //timestamp minus duration
    int64_t *output = (int64_t *)pOutputCol->pData;
    _getBigintValue_fn_t getVectorBigintValueFnLeft  = getVectorBigintValueFn(pLeftCol->info.type);
    _getBigintValue_fn_t getVectorBigintValueFnRight = getVectorBigintValueFn(pRightCol->info.type);

    if (pLeft->numOfRows == 1 && pRight->numOfRows == 1) {
      vectorMathTsSubHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, 1, i);
    } else if (pLeft->numOfRows == 1) {
      vectorMathTsSubHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, -1, i);
    } else if (pRight->numOfRows == 1) {
      vectorMathTsSubHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, 1, i);
    } else if (pLeft->numOfRows == pRight->numOfRows) {
      for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
        if (IS_NULL) {
          colDataAppendNULL(pOutputCol, i);
          continue;  // TODO set null or ignore
        }
        *output = getVectorBigintValueFnLeft(pLeftCol->pData, i) - getVectorBigintValueFnRight(pRightCol->pData, i);
      }
    }
  } else {
    double *output = (double *)pOutputCol->pData;
    _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
    _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

    if (pLeft->numOfRows == pRight->numOfRows) {
      for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
        if (IS_NULL) {
          colDataAppendNULL(pOutputCol, i);
          continue;  // TODO set null or ignore
        }
        *output = getVectorDoubleValueFnLeft(LEFT_COL, i) - getVectorDoubleValueFnRight(RIGHT_COL, i);
      }
    } else if (pLeft->numOfRows == 1) {
      vectorMathSubHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, -1, i);
    } else if (pRight->numOfRows == 1) {
      vectorMathSubHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, 1, i);
    }
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

// TODO not correct for descending order scan
static void vectorMathMultiplyHelper(SColumnInfoData* pLeftCol, SColumnInfoData* pRightCol, SColumnInfoData* pOutputCol, int32_t numOfRows, int32_t step, int32_t i) {
  _getDoubleValue_fn_t getVectorDoubleValueFnLeft  = getVectorDoubleValueFn(pLeftCol->info.type);
  _getDoubleValue_fn_t getVectorDoubleValueFnRight = getVectorDoubleValueFn(pRightCol->info.type);

  double *output = (double *)pOutputCol->pData;

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorDoubleValueFnLeft(LEFT_COL, i) * getVectorDoubleValueFnRight(RIGHT_COL, 0);
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
      if (IS_NULL) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorDoubleValueFnLeft(LEFT_COL, i) * getVectorDoubleValueFnRight(RIGHT_COL, i);
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
  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      if (IS_NULL || (getVectorDoubleValueFnRight(RIGHT_COL, i) == 0)) { //divide by 0 check
        colDataAppendNULL(pOutputCol, i);
        continue;
      }
      *output = getVectorDoubleValueFnLeft(LEFT_COL, i)
                / getVectorDoubleValueFnRight(RIGHT_COL, i);
    }
  } else if (pLeft->numOfRows == 1) {
    if (IS_HELPER_NULL(pLeftCol, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pRight->numOfRows);
    } else {
      for (; i >= 0 && i < pRight->numOfRows; i += step, output += 1) {
        if (IS_HELPER_NULL(pRightCol, i) || (getVectorDoubleValueFnRight(RIGHT_COL, i) == 0)) { // divide by 0 check
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        *output = getVectorDoubleValueFnLeft(LEFT_COL, 0)
                  / getVectorDoubleValueFnRight(RIGHT_COL, i);
      }
    }
  } else if (pRight->numOfRows == 1) {
    if (IS_HELPER_NULL(pRightCol, 0) || (getVectorDoubleValueFnRight(RIGHT_COL, 0) == 0)) {  // Set pLeft->numOfRows NULL value (divde by 0 check)
      colDataAppendNNULL(pOutputCol, 0, pLeft->numOfRows);
    } else {
      for (; i >= 0 && i < pLeft->numOfRows; i += step, output += 1) {
        if (IS_HELPER_NULL(pLeftCol, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }
        *output = getVectorDoubleValueFnLeft(LEFT_COL, i)
                  / getVectorDoubleValueFnRight(RIGHT_COL, 0);
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

  if (pLeft->numOfRows == pRight->numOfRows) {
    for (; i < pRight->numOfRows && i >= 0; i += step, output += 1) {
      if (IS_NULL) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }

      double lx = getVectorDoubleValueFnLeft(LEFT_COL, i);
      double rx = getVectorDoubleValueFnRight(RIGHT_COL, i);
      if (isnan(lx) || isinf(lx) || isnan(rx) || isinf(rx) || FLT_EQUAL(rx, 0)) {
        colDataAppendNULL(pOutputCol, i);
        continue;
      }

      *output = lx - ((int64_t)(lx / rx)) * rx;
    }
  } else if (pLeft->numOfRows == 1) {
    double lx = getVectorDoubleValueFnLeft(LEFT_COL, 0);
    if (IS_HELPER_NULL(pLeftCol, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pRight->numOfRows);
    } else {
      for (; i >= 0 && i < pRight->numOfRows; i += step, output += 1) {
        if (IS_HELPER_NULL(pRightCol, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        double rx = getVectorDoubleValueFnRight(RIGHT_COL, i);
        if (isnan(rx) || isinf(rx) || FLT_EQUAL(rx, 0)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        *output = lx - ((int64_t)(lx / rx)) * rx;
      }
    }
  } else if (pRight->numOfRows == 1) {
    double rx = getVectorDoubleValueFnRight(RIGHT_COL, 0);
    if (IS_HELPER_NULL(pRightCol, 0) || FLT_EQUAL(rx, 0)) {  // Set pLeft->numOfRows NULL value
      colDataAppendNNULL(pOutputCol, 0, pLeft->numOfRows);
    } else {
      for (; i >= 0 && i < pLeft->numOfRows; i += step, output += 1) {
        if (IS_HELPER_NULL(pLeftCol, i)) {
          colDataAppendNULL(pOutputCol, i);
          continue;
        }

        double lx = getVectorDoubleValueFnLeft(LEFT_COL, i);
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
    if (IS_HELPER_NULL(pLeftCol, i)) {
      colDataAppendNULL(pOutputCol, i);
      continue;
    }
    double result = getVectorDoubleValueFnLeft(LEFT_COL, i);
    *output = (result == 0) ? 0 : -result;
  }

  doReleaseVec(pLeftCol,  leftConvert);
}

void vectorAssign(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  pOut->numOfRows = pLeft->numOfRows;

//  if (IS_HELPER_NULL(pRight->columnData, 0)) {
  if(colDataIsNull_s(pRight->columnData, 0)){
    for (int32_t i = 0; i < pOut->numOfRows; ++i) {
      colDataAppend(pOutputCol, i, NULL, true);
    }
  } else {
    for (int32_t i = 0; i < pOut->numOfRows; ++i) {
      colDataAppend(pOutputCol, i, colDataGetData(pRight->columnData, 0), false);
    }
  }
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

  int64_t *output = (int64_t *)pOutputCol->pData;

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorBigintValueFnLeft(LEFT_COL, i) & getVectorBigintValueFnRight(RIGHT_COL, 0);
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
      if (IS_NULL) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorBigintValueFnLeft(LEFT_COL, i) & getVectorBigintValueFnRight(RIGHT_COL, i);
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

  if (IS_HELPER_NULL(pRightCol, 0)) {  // Set pLeft->numOfRows NULL value
    colDataAppendNNULL(pOutputCol, 0, numOfRows);
  } else {
    int64_t rx = getVectorBigintValueFnRight(RIGHT_COL, 0);
    for (; i >= 0 && i < numOfRows; i += step, output += 1) {
      if (IS_HELPER_NULL(pLeftCol, i)) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorBigintValueFnLeft(LEFT_COL, i) | rx;
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
      if (IS_NULL) {
        colDataAppendNULL(pOutputCol, i);
        continue;  // TODO set null or ignore
      }
      *output = getVectorBigintValueFnLeft(LEFT_COL, i) | getVectorBigintValueFnRight(RIGHT_COL, i);
    }
  } else if (pLeft->numOfRows == 1) {
    vectorBitOrHelper(pRightCol, pLeftCol, pOutputCol, pRight->numOfRows, step, i);
  } else if (pRight->numOfRows == 1) {
    vectorBitOrHelper(pLeftCol, pRightCol, pOutputCol, pLeft->numOfRows, step, i);
  }

  doReleaseVec(pLeftCol,  leftConvert);
  doReleaseVec(pRightCol, rightConvert);
}

#define VEC_COM_INNER(pCol, index1, index2)    \
  for (; i < pCol->numOfRows && i >= 0; i += step) {\
    if (IS_HELPER_NULL(pLeft->columnData, index1) || IS_HELPER_NULL(pRight->columnData, index2)) {\
      bool  res = false;\
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);\
      continue;\
    }\
    char *pLeftData = colDataGetData(pLeft->columnData, index1);\
    char *pRightData = colDataGetData(pRight->columnData, index2);\
    int64_t leftOut = 0;\
    int64_t rightOut = 0;\
    bool freeLeft = false;\
    bool freeRight = false;\
    bool isJsonnull = false;\
    bool result = convertJsonValue(&fp, optr, GET_PARAM_TYPE(pLeft), GET_PARAM_TYPE(pRight),\
                                      &pLeftData, &pRightData, &leftOut, &rightOut, &isJsonnull, &freeLeft, &freeRight);\
    if(isJsonnull){\
      ASSERT(0);\
    }\
    if(!pLeftData || !pRightData){\
      result = false;\
    }\
    if(!result){\
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&result);\
    }else{\
      bool  res = filterDoCompare(fp, optr, pLeftData, pRightData);\
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);\
    }\
    if(freeLeft) taosMemoryFreeClear(pLeftData);\
    if(freeRight) taosMemoryFreeClear(pRightData);\
  }

void vectorCompareImpl(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord, int32_t optr) {
  int32_t       i = ((_ord) == TSDB_ORDER_ASC) ? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t       step = ((_ord) == TSDB_ORDER_ASC) ? 1 : -1;

  __compar_fn_t fp = filterGetCompFunc(GET_PARAM_TYPE(pLeft), optr);
  if(terrno != TSDB_CODE_SUCCESS){
    return;
  }

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  if (pRight->pHashFilter != NULL) {
    for (; i >= 0 && i < pLeft->numOfRows; i += step) {
      if (IS_HELPER_NULL(pLeft->columnData, i)) {
        bool  res = false;
        colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
        continue;
      }

      char *pLeftData = colDataGetData(pLeft->columnData, i);
      bool  res = filterDoCompare(fp, optr, pLeftData, pRight->pHashFilter);
      colDataAppendInt8(pOut->columnData, i, (int8_t*)&res);
    }
    return;
  }

  if (pLeft->numOfRows == pRight->numOfRows) {
    VEC_COM_INNER(pLeft, i, i)
  } else if (pRight->numOfRows == 1) {
    VEC_COM_INNER(pLeft, i, 0)
  } else if (pLeft->numOfRows == 1) {
    VEC_COM_INNER(pRight, 0, i)
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
    int8_t v = IS_HELPER_NULL(pLeft->columnData, i) ? 1 : 0;
    colDataAppendInt8(pOut->columnData, i, &v);
  }
  pOut->numOfRows = pLeft->numOfRows;
}

void vectorNotNull(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  for(int32_t i = 0; i < pLeft->numOfRows; ++i) {
    int8_t v = IS_HELPER_NULL(pLeft->columnData, i) ? 0 : 1;
    colDataAppendInt8(pOut->columnData, i, &v);
  }
  pOut->numOfRows = pLeft->numOfRows;
}

void vectorIsTrue(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  vectorConvertImpl(pLeft, pOut, NULL);
  for(int32_t i = 0; i < pOut->numOfRows; ++i) {
    if(colDataIsNull_s(pOut->columnData, i)) {
      int8_t v = 0;
      colDataAppendInt8(pOut->columnData, i, &v);
      colDataSetNotNull_f(pOut->columnData->nullbitmap, i);
    }
  }
  pOut->columnData->hasNull = false;
}

STagVal getJsonValue(char *json, char *key, bool *isExist) {
  STagVal val = {.pKey = key};
  if (tTagIsJson((const STag *)json) == false){
    terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
    if(isExist){
      *isExist = false;
    }
    return val;
  }

  bool find = tTagGet(((const STag *)json), &val);  // json value is null and not exist is different
  if(isExist){
    *isExist = find;
  }
  return val;
}

void vectorJsonContains(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  char *pRightData = colDataGetVarData(pRight->columnData, 0);
  char *jsonKey = taosMemoryCalloc(1, varDataLen(pRightData) + 1);
  memcpy(jsonKey, varDataVal(pRightData), varDataLen(pRightData));
  for (; i >= 0 && i < pLeft->numOfRows; i += step) {
    bool isExist = false;

    if (!colDataIsNull_var(pLeft->columnData, i)) {
      char *pLeftData = colDataGetVarData(pLeft->columnData, i);
      getJsonValue(pLeftData, jsonKey, &isExist);
    }

    colDataAppend(pOutputCol, i, (const char*)(&isExist), false);

  }
  taosMemoryFree(jsonKey);
}

void vectorJsonArrow(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord) {
  SColumnInfoData *pOutputCol = pOut->columnData;

  int32_t i = ((_ord) == TSDB_ORDER_ASC)? 0 : TMAX(pLeft->numOfRows, pRight->numOfRows) - 1;
  int32_t step = ((_ord) == TSDB_ORDER_ASC)? 1 : -1;

  pOut->numOfRows = TMAX(pLeft->numOfRows, pRight->numOfRows);

  char *pRightData = colDataGetVarData(pRight->columnData, 0);
  char *jsonKey = taosMemoryCalloc(1, varDataLen(pRightData) + 1);
  memcpy(jsonKey, varDataVal(pRightData), varDataLen(pRightData));
  for (; i >= 0 && i < pLeft->numOfRows; i += step) {
    if (colDataIsNull_var(pLeft->columnData, i)) {
      colDataSetNull_var(pOutputCol, i);
      pOutputCol->hasNull = true;
      continue;
    }
    char *pLeftData = colDataGetVarData(pLeft->columnData, i);
    bool isExist = false;
    STagVal value = getJsonValue(pLeftData, jsonKey, &isExist);
    char *data = isExist ? tTagValToData(&value, true) : NULL;
    colDataAppend(pOutputCol, i, data, data == NULL);
    if(isExist && IS_VAR_DATA_TYPE(value.type) && data){
      taosMemoryFree(data);
    }
  }
  taosMemoryFree(jsonKey);
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
    case OP_TYPE_REM:
      return vectorMathRemainder;
    case OP_TYPE_MINUS:
      return vectorMathMinus;
    case OP_TYPE_ASSIGN:
      return vectorAssign;
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
    case OP_TYPE_JSON_GET_VALUE:
      return vectorJsonArrow;
    case OP_TYPE_JSON_CONTAINS:
      return vectorJsonContains;
    default:
      ASSERT(0);
      return NULL;
  }
}

