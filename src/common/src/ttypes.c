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

#include "taosdef.h"
#include "ttokendef.h"
#include "tscompression.h"

const int32_t TYPE_BYTES[11] = {
    -1,                      // TSDB_DATA_TYPE_NULL
    sizeof(int8_t),          // TSDB_DATA_TYPE_BOOL
    sizeof(int8_t),          // TSDB_DATA_TYPE_TINYINT
    sizeof(int16_t),         // TSDB_DATA_TYPE_SMALLINT
    sizeof(int32_t),         // TSDB_DATA_TYPE_INT
    sizeof(int64_t),         // TSDB_DATA_TYPE_BIGINT
    sizeof(float),           // TSDB_DATA_TYPE_FLOAT
    sizeof(double),          // TSDB_DATA_TYPE_DOUBLE
    sizeof(VarDataOffsetT),  // TSDB_DATA_TYPE_BINARY
    sizeof(TSKEY),           // TSDB_DATA_TYPE_TIMESTAMP
    sizeof(VarDataOffsetT)   // TSDB_DATA_TYPE_NCHAR
};

static void getStatics_bool(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                            int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int8_t *data = (int8_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((char *)&data[i], TSDB_DATA_TYPE_BOOL)) {
      (*numOfNull) += 1;
      continue;
    }
    
    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
      *minIndex = i;
    }
    
    if (*max < data[i]) {
      *max = data[i];
      *maxIndex = i;
    }
  }
}

static void getStatics_i8(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                          int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int8_t *data = (int8_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((char *)&data[i], TSDB_DATA_TYPE_TINYINT)) {
      (*numOfNull) += 1;
      continue;
    }
    
    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
      *minIndex = i;
    }
    
    if (*max < data[i]) {
      *max = data[i];
      *maxIndex = i;
    }
  }
}

static void getStatics_i16(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int16_t *data = (int16_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  //  int64_t lastKey = 0;
  //  int16_t lastVal = TSDB_DATA_SMALLINT_NULL;
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((const char*) &data[i], TSDB_DATA_TYPE_SMALLINT)) {
      (*numOfNull) += 1;
      continue;
    }
    
    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
      *minIndex = i;
    }
    
    if (*max < data[i]) {
      *max = data[i];
      *maxIndex = i;
    }
    
    //    if (isNull(&lastVal, TSDB_DATA_TYPE_SMALLINT)) {
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    } else {
    //      *wsum = lastVal * (primaryKey[i] - lastKey);
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    }
  }
}

static void getStatics_i32(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int32_t *data = (int32_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  //  int64_t lastKey = 0;
  //  int32_t lastVal = TSDB_DATA_INT_NULL;
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((const char*) &data[i], TSDB_DATA_TYPE_INT)) {
      (*numOfNull) += 1;
      continue;
    }
    
    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
      *minIndex = i;
    }
    
    if (*max < data[i]) {
      *max = data[i];
      *maxIndex = i;
    }
  }
}

static void getStatics_i64(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int64_t *data = (int64_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((const char*) &data[i], TSDB_DATA_TYPE_BIGINT)) {
      (*numOfNull) += 1;
      continue;
    }
    
    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
      *minIndex = i;
    }
    
    if (*max < data[i]) {
      *max = data[i];
      *maxIndex = i;
    }
    
    //    if (isNull(&lastVal, TSDB_DATA_TYPE_BIGINT)) {
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    } else {
    //      *wsum = lastVal * (primaryKey[i] - lastKey);
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    }
  }
}

static void getStatics_f(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                         int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  float *data = (float *)pData;
  float fmin      = FLT_MAX;
  float fmax      = -FLT_MAX;
  double dsum     = 0;
  *minIndex       = 0;
  *maxIndex       = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((const char*) &data[i], TSDB_DATA_TYPE_FLOAT)) {
      (*numOfNull) += 1;
      continue;
    }
    
    float fv = 0;
    fv = GET_FLOAT_VAL((const char*)&(data[i]));
    dsum += fv;
    if (fmin > fv) {
      fmin = fv;
      *minIndex = i;
    }
    
    if (fmax < fv) {
      fmax = fv;
      *maxIndex = i;
    }
  }
  
  double csum = 0;
  csum = GET_DOUBLE_VAL((const char *)sum);
  csum += dsum;

  SET_DOUBLE_VAL(sum, csum);
  SET_DOUBLE_VAL(max, fmax);
  SET_DOUBLE_VAL(min, fmin);
}

static void getStatics_d(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                         int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  double *data = (double *)pData;
  double dmin      = DBL_MAX;
  double dmax      = -DBL_MAX;
  double dsum      = 0;
  *minIndex        = 0;
  *maxIndex        = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((const char*) &data[i], TSDB_DATA_TYPE_DOUBLE)) {
      (*numOfNull) += 1;
      continue;
    }
    
    double dv = 0;
    dv = GET_DOUBLE_VAL((const char*)&(data[i]));
    dsum += dv;
    if (dmin > dv) {
      dmin = dv;
      *minIndex = i;
    }
    
    if (dmax < dv) {
      dmax = dv;
      *maxIndex = i;
    }
  }
  
  double csum = 0;
  csum = GET_DOUBLE_VAL((const char *)sum);
  csum += dsum;

  SET_DOUBLE_PTR(sum, &csum);
  SET_DOUBLE_PTR(max, &dmax);
  SET_DOUBLE_PTR(min, &dmin);
}

static void getStatics_bin(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                         int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  const char* data = pData;
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(data, TSDB_DATA_TYPE_BINARY)) {
      (*numOfNull) += 1;
    }
    
    data += varDataTLen(data);
  }
  
  *sum = 0;
  *max = 0;
  *min = 0;
  *minIndex = 0;
  *maxIndex = 0;
}

static void getStatics_nchr(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  const char* data = pData;
  ASSERT(numOfRow <= INT16_MAX);
  
  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(data, TSDB_DATA_TYPE_NCHAR)) {
      (*numOfNull) += 1;
    }
    
    data += varDataTLen(data);
  }
  
  *sum = 0;
  *max = 0;
  *min = 0;
  *minIndex = 0;
  *maxIndex = 0;
}

tDataTypeDescriptor tDataTypeDesc[11] = {
  {TSDB_DATA_TYPE_NULL,      6, 1,            "NOTYPE",    NULL,                NULL,                  NULL},
  {TSDB_DATA_TYPE_BOOL,      4, CHAR_BYTES,   "BOOL",      tsCompressBool,      tsDecompressBool,      getStatics_bool},
  {TSDB_DATA_TYPE_TINYINT,   7, CHAR_BYTES,   "TINYINT",   tsCompressTinyint,   tsDecompressTinyint,   getStatics_i8},
  {TSDB_DATA_TYPE_SMALLINT,  8, SHORT_BYTES,  "SMALLINT",  tsCompressSmallint,  tsDecompressSmallint,  getStatics_i16},
  {TSDB_DATA_TYPE_INT,       3, INT_BYTES,    "INT",       tsCompressInt,       tsDecompressInt,       getStatics_i32},
  {TSDB_DATA_TYPE_BIGINT,    6, LONG_BYTES,   "BIGINT",    tsCompressBigint,    tsDecompressBigint,    getStatics_i64},
  {TSDB_DATA_TYPE_FLOAT,     5, FLOAT_BYTES,  "FLOAT",     tsCompressFloat,     tsDecompressFloat,     getStatics_f},
  {TSDB_DATA_TYPE_DOUBLE,    6, DOUBLE_BYTES, "DOUBLE",    tsCompressDouble,    tsDecompressDouble,    getStatics_d},
  {TSDB_DATA_TYPE_BINARY,    6, 0,            "BINARY",    tsCompressString,    tsDecompressString,    getStatics_bin},
  {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES,   "TIMESTAMP", tsCompressTimestamp, tsDecompressTimestamp, getStatics_i64},
  {TSDB_DATA_TYPE_NCHAR,     5, 8,            "NCHAR",     tsCompressString,    tsDecompressString,    getStatics_nchr},
};

char tTokenTypeSwitcher[13] = {
    TSDB_DATA_TYPE_NULL,    // no type
    TSDB_DATA_TYPE_BINARY,  // TK_ID
    TSDB_DATA_TYPE_BOOL,    // TK_BOOL
    TSDB_DATA_TYPE_BIGINT,  // TK_TINYINT
    TSDB_DATA_TYPE_BIGINT,  // TK_SMALLINT
    TSDB_DATA_TYPE_BIGINT,  // TK_INTEGER
    TSDB_DATA_TYPE_BIGINT,  // TK_BIGINT
    TSDB_DATA_TYPE_DOUBLE,  // TK_FLOAT
    TSDB_DATA_TYPE_DOUBLE,  // TK_DOUBLE
    TSDB_DATA_TYPE_BINARY,  // TK_STRING
    TSDB_DATA_TYPE_BIGINT,  // TK_TIMESTAMP
    TSDB_DATA_TYPE_BINARY,  // TK_BINARY
    TSDB_DATA_TYPE_NCHAR,   // TK_NCHAR
};

bool isValidDataType(int32_t type) {
  return type >= TSDB_DATA_TYPE_NULL && type <= TSDB_DATA_TYPE_NCHAR;
}

void setVardataNull(char* val, int32_t type) {
  if (type == TSDB_DATA_TYPE_BINARY) {
    varDataSetLen(val, sizeof(int8_t));
    *(uint8_t*) varDataVal(val) = TSDB_DATA_BINARY_NULL;
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    varDataSetLen(val, sizeof(int32_t));
    *(uint32_t*) varDataVal(val) = TSDB_DATA_NCHAR_NULL;
  } else {
    assert(0);
  }
}

void setNull(char *val, int32_t type, int32_t bytes) { setNullN(val, type, bytes, 1); }

void setNullN(char *val, int32_t type, int32_t bytes, int32_t numOfElems) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_BOOL_NULL;
      }
      break;
    case TSDB_DATA_TYPE_TINYINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_TINYINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint16_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_SMALLINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_INT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_INT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_BIGINT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_FLOAT:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_FLOAT_NULL;
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint64_t *)(val + i * tDataTypeDesc[type].nSize) = TSDB_DATA_DOUBLE_NULL;
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY:
      for (int32_t i = 0; i < numOfElems; ++i) {
        setVardataNull(val + i * bytes, type);
      }
      break;
    default: {
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize) = TSDB_DATA_INT_NULL;
      }
      break;
    }
  }
}

static uint8_t  nullBool = TSDB_DATA_BOOL_NULL;
static uint8_t  nullTinyInt = TSDB_DATA_TINYINT_NULL;
static uint16_t nullSmallInt = TSDB_DATA_SMALLINT_NULL;
static uint32_t nullInt = TSDB_DATA_INT_NULL;
static uint64_t nullBigInt = TSDB_DATA_BIGINT_NULL;
static uint32_t nullFloat = TSDB_DATA_FLOAT_NULL;
static uint64_t nullDouble = TSDB_DATA_DOUBLE_NULL;

static union {
  tstr str;
  char pad[sizeof(tstr) + 4];
} nullBinary = {.str = {.len = 1}}, nullNchar = {.str = {.len = 4}};

static void *nullValues[] = {
    &nullBool,  &nullTinyInt, &nullSmallInt, &nullInt,    &nullBigInt,
    &nullFloat, &nullDouble,  &nullBinary,   &nullBigInt, &nullNchar,
};

void *getNullValue(int32_t type) {
  assert(type >= TSDB_DATA_TYPE_BOOL && type <= TSDB_DATA_TYPE_NCHAR);
  return nullValues[type - 1];
}

void assignVal(char *val, const char *src, int32_t len, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *((int32_t *)val) = GET_INT32_VAL(src);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
      SET_FLOAT_VAL(val, GET_FLOAT_VAL(src));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(val, GET_DOUBLE_VAL(src));
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)val) = GET_INT64_VAL(src);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)val) = GET_INT16_VAL(src);
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)val) = GET_INT8_VAL(src);
      break;
    case TSDB_DATA_TYPE_BINARY:
      varDataCopy(val, src);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      varDataCopy(val, src);
      break;
    default: {
      memcpy(val, src, len);
      break;
    }
  }
}

void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size, void* buf) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      SWAP(*(int32_t *)(pLeft), *(int32_t *)(pRight), int32_t);
      break;
    }
    
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      SWAP(*(int64_t *)(pLeft), *(int64_t *)(pRight), int64_t);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      SWAP(*(double *)(pLeft), *(double *)(pRight), double);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      SWAP(*(int16_t *)(pLeft), *(int16_t *)(pRight), int16_t);
      break;
    }
    
    case TSDB_DATA_TYPE_FLOAT: {
      SWAP(*(float *)(pLeft), *(float *)(pRight), float);
      break;
    }
    
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      SWAP(*(int8_t *)(pLeft), *(int8_t *)(pRight), int8_t);
      break;
    }
    
    default: {
      memcpy(buf, pLeft, size);
      memcpy(pLeft, pRight, size);
      memcpy(pRight, buf, size);
      break;
    }
  }
}
