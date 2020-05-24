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

static void getStatics_i8(const TSKEY *primaryKey, const void *pData, int32_t numOfRow, int64_t *min, int64_t *max,
                          int64_t *sum, int16_t *minIndex, int16_t *maxIndex, int16_t *numOfNull) {
  int8_t *data = (int8_t *)pData;
  *min = INT64_MAX;
  *max = INT64_MIN;
  *minIndex = 0;
  *maxIndex = 0;
  
  ASSERT(numOfRow <= INT16_MAX);
  
  //  int64_t lastKey = 0;
  //  int8_t  lastVal = TSDB_DATA_TINYINT_NULL;
  
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
    
    //    if (isNull(&lastVal, TSDB_DATA_TYPE_INT)) {
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    } else {
    //      *wsum = lastVal * (primaryKey[i] - lastKey);
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    }
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
  float fmin      = DBL_MAX;
  float fmax      = -DBL_MAX;
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
    fv = GET_FLOAT_VAL(&(data[i]));
    dsum += fv;
    if (fmin > fv) {
      fmin = fv;
      *minIndex = i;
    }
    
    if (fmax < fv) {
      fmax = fv;
      *maxIndex = i;
    }
    
    //    if (isNull(&lastVal, TSDB_DATA_TYPE_FLOAT)) {
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    } else {
    //      *wsum = lastVal * (primaryKey[i] - lastKey);
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    }
  }
  
  double csum = 0;
  csum = GET_DOUBLE_VAL(sum);
  csum += dsum;
#ifdef _TD_ARM_32_
  SET_DOUBLE_VAL_ALIGN(sum, &csum);
  SET_DOUBLE_VAL_ALIGN(max, &fmax);
  SET_DOUBLE_VAL_ALIGN(min, &fmin);
#else
  *sum = csum;
  *max = fmax;
  *min = fmin;
#endif
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
    dv = GET_DOUBLE_VAL(&(data[i]));
    dsum += dv;
    if (dmin > dv) {
      dmin = dv;
      *minIndex = i;
    }
    
    if (dmax < dv) {
      dmax = dv;
      *maxIndex = i;
    }
    
    //    if (isNull(&lastVal, TSDB_DATA_TYPE_DOUBLE)) {
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    } else {
    //      *wsum = lastVal * (primaryKey[i] - lastKey);
    //      lastKey = primaryKey[i];
    //      lastVal = data[i];
    //    }
  }
  
  double csum = 0;
  csum = GET_DOUBLE_VAL(sum);
  csum += dsum;


#ifdef _TD_ARM_32_
  SET_DOUBLE_VAL_ALIGN(sum, &csum);
    SET_DOUBLE_VAL_ALIGN(max, &dmax);
    SET_DOUBLE_VAL_ALIGN(min, &dmin);
#else
  *sum = csum;
  *max = dmax;
  *min = dmin;
#endif
}

tDataTypeDescriptor tDataTypeDesc[11] = {
  {TSDB_DATA_TYPE_NULL,      6, 1,            "NOTYPE",    NULL,                NULL,                  NULL},
  {TSDB_DATA_TYPE_BOOL,      4, CHAR_BYTES,   "BOOL",      tsCompressBool,      tsDecompressBool,      NULL},
  {TSDB_DATA_TYPE_TINYINT,   7, CHAR_BYTES,   "TINYINT",   tsCompressTinyint,   tsDecompressTinyint,   getStatics_i8},
  {TSDB_DATA_TYPE_SMALLINT,  8, SHORT_BYTES,  "SMALLINT",  tsCompressSmallint,  tsDecompressSmallint,  getStatics_i16},
  {TSDB_DATA_TYPE_INT,       3, INT_BYTES,    "INT",       tsCompressInt,       tsDecompressInt,       getStatics_i32},
  {TSDB_DATA_TYPE_BIGINT,    6, LONG_BYTES,   "BIGINT",    tsCompressBigint,    tsDecompressBigint,    getStatics_i64},
  {TSDB_DATA_TYPE_FLOAT,     5, FLOAT_BYTES,  "FLOAT",     tsCompressFloat,     tsDecompressFloat,     getStatics_f},
  {TSDB_DATA_TYPE_DOUBLE,    6, DOUBLE_BYTES, "DOUBLE",    tsCompressDouble,    tsDecompressDouble,    getStatics_d},
  {TSDB_DATA_TYPE_BINARY,    6, 0,            "BINARY",    tsCompressString,    tsDecompressString,    NULL},
  {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES,   "TIMESTAMP", tsCompressTimestamp, tsDecompressTimestamp, getStatics_i64},
  {TSDB_DATA_TYPE_NCHAR,     5, 8,            "NCHAR",     tsCompressString,    tsDecompressString,    NULL},
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

bool isValidDataType(int32_t type, int32_t length) {
  if (type < TSDB_DATA_TYPE_BOOL || type > TSDB_DATA_TYPE_NCHAR) {
    return false;
  }

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
//    return length >= 0 && length <= TSDB_MAX_BINARY_LEN;
  }

  return true;
}

bool isNull(const char *val, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return *(uint8_t *)val == TSDB_DATA_BOOL_NULL;
    case TSDB_DATA_TYPE_TINYINT:
      return *(uint8_t *)val == TSDB_DATA_TINYINT_NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      return *(uint16_t *)val == TSDB_DATA_SMALLINT_NULL;
    case TSDB_DATA_TYPE_INT:
      return *(uint32_t *)val == TSDB_DATA_INT_NULL;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return *(uint64_t *)val == TSDB_DATA_BIGINT_NULL;
    case TSDB_DATA_TYPE_FLOAT:
      return *(uint32_t *)val == TSDB_DATA_FLOAT_NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      return *(uint64_t *)val == TSDB_DATA_DOUBLE_NULL;
    case TSDB_DATA_TYPE_NCHAR:
      return *(uint32_t*) varDataVal(val) == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return *(uint8_t *) varDataVal(val) == TSDB_DATA_BINARY_NULL;
    default:
      return false;
  };
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
    case TSDB_DATA_TYPE_NCHAR: // todo : without length?
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint32_t *)(val + i * bytes) = TSDB_DATA_NCHAR_NULL;
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
      for (int32_t i = 0; i < numOfElems; ++i) {
        *(uint8_t *)(val + i * bytes) = TSDB_DATA_BINARY_NULL;
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

void assignVal(char *val, const char *src, int32_t len, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      *((int32_t *)val) = GET_INT32_VAL(src);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
#ifdef _TD_ARM_32_
      float fv = GET_FLOAT_VAL(src);
      SET_FLOAT_VAL_ALIGN(val, &fv);
#else
      *((float *)val) = GET_FLOAT_VAL(src);
#endif
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
#ifdef _TD_ARM_32_
      double dv = GET_DOUBLE_VAL(src);
      SET_DOUBLE_VAL_ALIGN(val, &dv);
#else
      *((double *)val) = GET_DOUBLE_VAL(src);
#endif
      break;
    };
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: {
      *((int64_t *)val) = GET_INT64_VAL(src);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      *((int16_t *)val) = GET_INT16_VAL(src);
      break;
    };
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      *((int8_t *)val) = GET_INT8_VAL(src);
      break;
    };
    case TSDB_DATA_TYPE_BINARY: {
      varDataCopy(val, src);
      break;
    };
    case TSDB_DATA_TYPE_NCHAR: {
      wcsncpy((wchar_t*)val, (wchar_t*)src, len / TSDB_NCHAR_SIZE);
      break;
    };
    default: {
      memcpy(val, src, len);
      break;
    }
  }
}

void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size) {
  char tmpBuf[4096] = {0};
  
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
      assert(size <= 4096);
      memcpy(tmpBuf, pLeft, size);
      memcpy(pLeft, pRight, size);
      memcpy(pRight, tmpBuf, size);
      break;
    }
  }
}