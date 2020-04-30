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

tDataTypeDescriptor tDataTypeDesc[11] = {
  {TSDB_DATA_TYPE_NULL,      6, 1,            "NOTYPE",    NULL,                NULL},
  {TSDB_DATA_TYPE_BOOL,      4, CHAR_BYTES,   "BOOL",      tsCompressBool,      tsDecompressBool},
  {TSDB_DATA_TYPE_TINYINT,   7, CHAR_BYTES,   "TINYINT",   tsCompressTinyint,   tsDecompressTinyint},
  {TSDB_DATA_TYPE_SMALLINT,  8, SHORT_BYTES,  "SMALLINT",  tsCompressSmallint,  tsDecompressSmallint},
  {TSDB_DATA_TYPE_INT,       3, INT_BYTES,    "INT",       tsCompressInt,       tsDecompressInt},
  {TSDB_DATA_TYPE_BIGINT,    6, LONG_BYTES,   "BIGINT",    tsCompressBigint,    tsDecompressBigint},
  {TSDB_DATA_TYPE_FLOAT,     5, FLOAT_BYTES,  "FLOAT",     tsCompressFloat,     tsDecompressFloat},
  {TSDB_DATA_TYPE_DOUBLE,    6, DOUBLE_BYTES, "DOUBLE",    tsCompressDouble,    tsDecompressDouble},
  {TSDB_DATA_TYPE_BINARY,    6, 0,            "BINARY",    tsCompressString,    tsDecompressString},
  {TSDB_DATA_TYPE_TIMESTAMP, 9, LONG_BYTES,   "TIMESTAMP", tsCompressTimestamp, tsDecompressTimestamp},
  {TSDB_DATA_TYPE_NCHAR,     5, 8,            "NCHAR",     tsCompressString,    tsDecompressString},
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
      return *(uint32_t *)val == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return *(uint8_t *)val == TSDB_DATA_BINARY_NULL;
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
    case TSDB_DATA_TYPE_NCHAR:
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
      strncpy(val, src, len);
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