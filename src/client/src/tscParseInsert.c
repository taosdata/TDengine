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

#define _DEFAULT_SOURCE /* See feature_test_macros(7) */
#define _GNU_SOURCE

#define _XOPEN_SOURCE

#include "os.h"

#include "hash.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "ttokendef.h"
#include "taosdef.h"

#include "tscLog.h"
#include "tscSubquery.h"
#include "tstoken.h"
#include "ttime.h"

#include "tdataformat.h"

enum {
  TSDB_USE_SERVER_TS = 0,
  TSDB_USE_CLI_TS = 1,
};

static int32_t tscAllocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t * numOfRows);

static int32_t tscToInteger(SSQLToken *pToken, int64_t *value, char **endPtr) {
  if (pToken->n == 0) {
    return TK_ILLEGAL;
  }
  

  int32_t radix = 10;
  if (pToken->type == TK_HEX) {
    radix = 16;
  } else if (pToken->type == TK_BIN) {
    radix = 2;
  }
  
  errno = 0;
  *value = strtoll(pToken->z, endPtr, radix);
  if (**endPtr == 'e' || **endPtr == 'E' || **endPtr == '.') {
    errno = 0;
    double v = round(strtod(pToken->z, endPtr));
    if (v > INT64_MAX || v <= INT64_MIN) {
      errno = ERANGE;
    } else {
      *value = v;
    }
  }
  
  // not a valid integer number, return error
  if (*endPtr - pToken->z != pToken->n) {
    return TK_ILLEGAL;
  }

  return pToken->type;
}

static int32_t tscToDouble(SSQLToken *pToken, double *value, char **endPtr) {
  if (pToken->n == 0) {
    return TK_ILLEGAL;
  }
  
  errno = 0;
  *value = strtod(pToken->z, endPtr);
  
  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_ILLEGAL;
  }

  return pToken->type;
}

int tsParseTime(SSQLToken *pToken, int64_t *time, char **next, char *error, int16_t timePrec) {
  int32_t   index = 0;
  SSQLToken sToken;
  int64_t   interval;
  int64_t   useconds = 0;
  char *    pTokenEnd = *next;

  index = 0;

  if (pToken->type == TK_NOW) {
    useconds = taosGetTimestamp(timePrec);
  } else if (strncmp(pToken->z, "0", 1) == 0 && pToken->n == 1) {
    // do nothing
  } else if (pToken->type == TK_INTEGER) {
    useconds = tsosStr2int64(pToken->z);
  } else {
    // strptime("2001-11-12 18:31:01", "%Y-%m-%d %H:%M:%S", &tm);
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return tscInvalidSQLErrMsg(error, "invalid timestamp format", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = pToken->n; pToken->z[k] != '\0'; k++) {
    if (pToken->z[k] == ' ' || pToken->z[k] == '\t') continue;
    if (pToken->z[k] == ',') {
      *next = pTokenEnd;
      *time = useconds;
      return 0;
    }

    break;
  }

  /*
   * time expression:
   * e.g., now+12a, now-5h
   */
  SSQLToken valueToken;
  index = 0;
  sToken = tStrGetToken(pTokenEnd, &index, false, 0, NULL);
  pTokenEnd += index;

  if (sToken.type == TK_MINUS || sToken.type == TK_PLUS) {
    index = 0;
    valueToken = tStrGetToken(pTokenEnd, &index, false, 0, NULL);
    pTokenEnd += index;

    if (valueToken.n < 2) {
      return tscInvalidSQLErrMsg(error, "value expected in timestamp", sToken.z);
    }

    if (getTimestampInUsFromStr(valueToken.z, valueToken.n, &interval) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (timePrec == TSDB_TIME_PRECISION_MILLI) {
      interval /= 1000;
    }

    if (sToken.type == TK_PLUS) {
      useconds += interval;
    } else {
      useconds = (useconds >= interval) ? useconds - interval : 0;
    }

    *next = pTokenEnd;
  }

  *time = useconds;
  return TSDB_CODE_SUCCESS;
}

int32_t tsParseOneColumnData(SSchema *pSchema, SSQLToken *pToken, char *payload, char *msg, char **str, bool primaryKey,
                             int16_t timePrec) {
  int64_t iv;
  int32_t numType;
  char *  endptr = NULL;
  errno = 0;  // clear the previous existed error information

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          *(uint8_t *)payload = TSDB_TRUE;
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          *(uint8_t *)payload = TSDB_FALSE;
        } else if (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0) {
          *(uint8_t *)payload = TSDB_DATA_BOOL_NULL;
        } else {
          return tscInvalidSQLErrMsg(msg, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_INTEGER) {
        iv = strtoll(pToken->z, NULL, 10);
        *(uint8_t *)payload = (int8_t)((iv == 0) ? TSDB_FALSE : TSDB_TRUE);
      } else if (pToken->type == TK_FLOAT) {
        double dv = strtod(pToken->z, NULL);
        *(uint8_t *)payload = (int8_t)((dv == 0) ? TSDB_FALSE : TSDB_TRUE);
      } else if (pToken->type == TK_NULL) {
        *(uint8_t *)payload = TSDB_DATA_BOOL_NULL;
      } else {
        return tscInvalidSQLErrMsg(msg, "invalid bool data", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT:
      if (pToken->type == TK_NULL) {
        *((int8_t *)payload) = TSDB_DATA_TINYINT_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int8_t *)payload) = TSDB_DATA_TINYINT_NULL;
      } else {
        numType = tscToInteger(pToken, &iv, &endptr);
        if (TK_ILLEGAL == numType) {
          return tscInvalidSQLErrMsg(msg, "invalid tinyint data", pToken->z);
        } else if (errno == ERANGE || iv > INT8_MAX || iv <= INT8_MIN) {
          return tscInvalidSQLErrMsg(msg, "tinyint data overflow", pToken->z);
        }

        *((int8_t *)payload) = (int8_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_SMALLINT:
      if (pToken->type == TK_NULL) {
        *((int16_t *)payload) = TSDB_DATA_SMALLINT_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int16_t *)payload) = TSDB_DATA_SMALLINT_NULL;
      } else {
        numType = tscToInteger(pToken, &iv, &endptr);
        if (TK_ILLEGAL == numType) {
          return tscInvalidSQLErrMsg(msg, "invalid smallint data", pToken->z);
        } else if (errno == ERANGE || iv > INT16_MAX || iv <= INT16_MIN) {
          return tscInvalidSQLErrMsg(msg, "smallint data overflow", pToken->z);
        }

        *((int16_t *)payload) = (int16_t)iv;
      }
      break;

    case TSDB_DATA_TYPE_INT:
      if (pToken->type == TK_NULL) {
        *((int32_t *)payload) = TSDB_DATA_INT_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int32_t *)payload) = TSDB_DATA_INT_NULL;
      } else {
        numType = tscToInteger(pToken, &iv, &endptr);
        if (TK_ILLEGAL == numType) {
          return tscInvalidSQLErrMsg(msg, "invalid int data", pToken->z);
        } else if (errno == ERANGE || iv > INT32_MAX || iv <= INT32_MIN) {
          return tscInvalidSQLErrMsg(msg, "int data overflow", pToken->z);
        }

        *((int32_t *)payload) = (int32_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (pToken->type == TK_NULL) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else {
        numType = tscToInteger(pToken, &iv, &endptr);
        if (TK_ILLEGAL == numType) {
          return tscInvalidSQLErrMsg(msg, "invalid bigint data", pToken->z);
        } else if (errno == ERANGE || iv == INT64_MIN) {
          return tscInvalidSQLErrMsg(msg, "bigint data overflow", pToken->z);
        }

        *((int64_t *)payload) = iv;
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (pToken->type == TK_NULL) {
        *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidSQLErrMsg(msg, "illegal float data", pToken->z);
        }

        float fv = (float)dv;
        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || (fv > FLT_MAX || fv < -FLT_MAX)) {
          return tscInvalidSQLErrMsg(msg, "illegal float data", pToken->z);
        }

        if (isinf(fv) || isnan(fv)) {
          *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
        }

        *((float *)payload) = fv;
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (pToken->type == TK_NULL) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
      } else if ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                 (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0)) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidSQLErrMsg(msg, "illegal double data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || (dv > DBL_MAX || dv < -DBL_MAX)) {
          return tscInvalidSQLErrMsg(msg, "illegal double data", pToken->z);
        }

        if (isinf(dv) || isnan(dv)) {
          *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
        } else {
          *((double *)payload) = dv;
        }
      }
      break;

    case TSDB_DATA_TYPE_BINARY:
      // binary data cannot be null-terminated char string, otherwise the last char of the string is lost
      if (pToken->type == TK_NULL) {
        setVardataNull(payload, TSDB_DATA_TYPE_BINARY);
      } else { // too long values will return invalid sql, not be truncated automatically
        if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) { //todo refactor
          return tscInvalidSQLErrMsg(msg, "string data overflow", pToken->z);
        }
        
        STR_WITH_SIZE_TO_VARSTR(payload, pToken->z, pToken->n);
      }

      break;

    case TSDB_DATA_TYPE_NCHAR:
      if (pToken->type == TK_NULL) {
        setVardataNull(payload, TSDB_DATA_TYPE_NCHAR);
      } else {
        // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
        size_t output = 0;
        if (!taosMbsToUcs4(pToken->z, pToken->n, varDataVal(payload), pSchema->bytes - VARSTR_HEADER_SIZE, &output)) {
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), "%s", strerror(errno));
          return tscInvalidSQLErrMsg(msg, buf, pToken->z);
        }
        
        varDataSetLen(payload, output);
      }
      break;

    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (pToken->type == TK_NULL) {
        if (primaryKey) {
          *((int64_t *)payload) = 0;
        } else {
          *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
        }
      } else {
        int64_t temp;
        if (tsParseTime(pToken, &temp, str, msg, timePrec) != TSDB_CODE_SUCCESS) {
          return tscInvalidSQLErrMsg(msg, "invalid timestamp", pToken->z);
        }
        
        *((int64_t *)payload) = temp;
      }

      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * The server time/client time should not be mixed up in one sql string
 * Do not employ sort operation is not involved if server time is used.
 */
static int32_t tsCheckTimestamp(STableDataBlocks *pDataBlocks, const char *start) {
  // once the data block is disordered, we do NOT keep previous timestamp any more
  if (!pDataBlocks->ordered) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY k = *(TSKEY *)start;

  if (k == 0) {
    if (pDataBlocks->tsSource == TSDB_USE_CLI_TS) {
      return -1;
    } else if (pDataBlocks->tsSource == -1) {
      pDataBlocks->tsSource = TSDB_USE_SERVER_TS;
    }
  } else {
    if (pDataBlocks->tsSource == TSDB_USE_SERVER_TS) {
      return -1;  // client time/server time can not be mixed

    } else if (pDataBlocks->tsSource == -1) {
      pDataBlocks->tsSource = TSDB_USE_CLI_TS;
    }
  }

  if (k <= pDataBlocks->prevTS && (pDataBlocks->tsSource == TSDB_USE_CLI_TS)) {
    pDataBlocks->ordered = false;
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

int tsParseOneRowData(char **str, STableDataBlocks *pDataBlocks, SSchema schema[], SParsedDataColInfo *spd, char *error,
                      int16_t timePrec, int32_t *code, char *tmpTokenBuf) {
  int32_t index = 0;
  SSQLToken sToken = {0};
  char *    payload = pDataBlocks->pData + pDataBlocks->size;

  // 1. set the parsed value from sql string
  int32_t rowSize = 0;
  for (int i = 0; i < spd->numOfAssignedCols; ++i) {
    // the start position in data block buffer of current value in sql
    char *   start = payload + spd->elems[i].offset;
    int16_t  colIndex = spd->elems[i].colIndex;
    SSchema *pSchema = schema + colIndex;
    rowSize += pSchema->bytes;

    index = 0;
    sToken = tStrGetToken(*str, &index, true, 0, NULL);
    *str += index;

    if (sToken.type == TK_QUESTION) {
      uint32_t offset = start - pDataBlocks->pData;
      if (tscAddParamToDataBlock(pDataBlocks, pSchema->type, (uint8_t)timePrec, pSchema->bytes, offset) != NULL) {
        continue;
      }

      strcpy(error, "client out of memory");
      *code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return -1;
    }

    int16_t type = sToken.type;
    if ((type != TK_NOW && type != TK_INTEGER && type != TK_STRING && type != TK_FLOAT && type != TK_BOOL &&
         type != TK_NULL && type != TK_HEX && type != TK_OCT && type != TK_BIN) || (sToken.n == 0) || (type == TK_RP)) {
      tscInvalidSQLErrMsg(error, "invalid data or symbol", sToken.z);
      *code = TSDB_CODE_TSC_INVALID_SQL;
      return -1;
    }

    // Remove quotation marks
    if (TK_STRING == sToken.type) {
      // delete escape character: \\, \', \"
      char    delim = sToken.z[0];
      int32_t cnt = 0;
      int32_t j = 0;
      for (int32_t k = 1; k < sToken.n - 1; ++k) {
        if (sToken.z[k] == delim || sToken.z[k] == '\\') {
          if (sToken.z[k + 1] == delim) {
            cnt++;
            tmpTokenBuf[j] = sToken.z[k + 1];
            j++;
            k++;
            continue;
          }
        }

        tmpTokenBuf[j] = sToken.z[k];
        j++;
      }
      tmpTokenBuf[j] = 0;
      sToken.z = tmpTokenBuf;
      sToken.n -= 2 + cnt;
    }

    bool    isPrimaryKey = (colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX);
    int32_t ret = tsParseOneColumnData(pSchema, &sToken, start, error, str, isPrimaryKey, timePrec);
    if (ret != TSDB_CODE_SUCCESS) {
      *code = TSDB_CODE_TSC_INVALID_SQL;
      return -1;  // NOTE: here 0 mean error!
    }

    if (isPrimaryKey && tsCheckTimestamp(pDataBlocks, start) != TSDB_CODE_SUCCESS) {
      tscInvalidSQLErrMsg(error, "client time/server time can not be mixed up", sToken.z);
      *code = TSDB_CODE_TSC_INVALID_TIME_STAMP;
      return -1;
    }
  }

  // 2. set the null value for the columns that do not assign values
  if (spd->numOfAssignedCols < spd->numOfCols) {
    char *ptr = payload;

    for (int32_t i = 0; i < spd->numOfCols; ++i) {
      
      if (!spd->hasVal[i]) {  // current column do not have any value to insert, set it to null
        if (schema[i].type == TSDB_DATA_TYPE_BINARY) {
          varDataSetLen(ptr, sizeof(int8_t));
          *(uint8_t*) varDataVal(ptr) = TSDB_DATA_BINARY_NULL;
        } else if (schema[i].type == TSDB_DATA_TYPE_NCHAR) {
          varDataSetLen(ptr, sizeof(int32_t));
          *(uint32_t*) varDataVal(ptr) = TSDB_DATA_NCHAR_NULL;
        } else {
          setNull(ptr, schema[i].type, schema[i].bytes);
        }
      }
      
      ptr += schema[i].bytes;
    }

    rowSize = ptr - payload;
  }

  return rowSize;
}

static int32_t rowDataCompar(const void *lhs, const void *rhs) {
  TSKEY left = *(TSKEY *)lhs;
  TSKEY right = *(TSKEY *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int tsParseValues(char **str, STableDataBlocks *pDataBlock, STableMeta *pTableMeta, int maxRows,
                  SParsedDataColInfo *spd, char *error, int32_t *code, char *tmpTokenBuf) {
  int32_t   index = 0;
  SSQLToken sToken;

  int16_t numOfRows = 0;

  SSchema *pSchema = tscGetTableSchema(pTableMeta);
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  
  int32_t  precision = tinfo.precision;

  if (spd->hasVal[0] == false) {
    strcpy(error, "primary timestamp column can not be null");
    *code = TSDB_CODE_TSC_INVALID_SQL;
    return -1;
  }

  while (1) {
    index = 0;
    sToken = tStrGetToken(*str, &index, false, 0, NULL);
    if (sToken.n == 0 || sToken.type != TK_LP) break;

    *str += index;
    if (numOfRows >= maxRows || pDataBlock->size + tinfo.rowSize >= pDataBlock->nAllocSize) {
      int32_t tSize;
      int32_t retcode = tscAllocateMemIfNeed(pDataBlock, tinfo.rowSize, &tSize);
      if (retcode != TSDB_CODE_SUCCESS) {  //TODO pass the correct error code to client
        strcpy(error, "client out of memory");
        *code = retcode;
        return -1;
      }
      ASSERT(tSize > maxRows);
      maxRows = tSize;
    }

    int32_t len = tsParseOneRowData(str, pDataBlock, pSchema, spd, error, precision, code, tmpTokenBuf);
    if (len <= 0) {  // error message has been set in tsParseOneRowData
      return -1;
    }

    pDataBlock->size += len;

    index = 0;
    sToken = tStrGetToken(*str, &index, false, 0, NULL);
    *str += index;
    if (sToken.n == 0 || sToken.type != TK_RP) {
      tscInvalidSQLErrMsg(error, ") expected", *str);
      *code = TSDB_CODE_TSC_INVALID_SQL;
      return -1;
    }

    numOfRows++;
  }

  if (numOfRows <= 0) {
    strcpy(error, "no any data points");
    *code = TSDB_CODE_TSC_INVALID_SQL;
    return -1;
  } else {
    return numOfRows;
  }
}

static void tscSetAssignedColumnInfo(SParsedDataColInfo *spd, SSchema *pSchema, int32_t numOfCols) {
  spd->numOfCols = numOfCols;
  spd->numOfAssignedCols = numOfCols;

  for (int32_t i = 0; i < numOfCols; ++i) {
    spd->hasVal[i] = true;
    spd->elems[i].colIndex = i;

    if (i > 0) {
      spd->elems[i].offset = spd->elems[i - 1].offset + pSchema[i - 1].bytes;
    }
  }
}

int32_t tscAllocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t * numOfRows) {
  size_t    remain = pDataBlock->nAllocSize - pDataBlock->size;
  const int factor = 5;
  uint32_t nAllocSizeOld = pDataBlock->nAllocSize;
  
  // expand the allocated size
  if (remain < rowSize * factor) {
    while (remain < rowSize * factor) {
      pDataBlock->nAllocSize = (uint32_t)(pDataBlock->nAllocSize * 1.5);
      remain = pDataBlock->nAllocSize - pDataBlock->size;
    }

    char *tmp = realloc(pDataBlock->pData, (size_t)pDataBlock->nAllocSize);
    if (tmp != NULL) {
      pDataBlock->pData = tmp;
      memset(pDataBlock->pData + pDataBlock->size, 0, pDataBlock->nAllocSize - pDataBlock->size);
    } else {
      // do nothing, if allocate more memory failed
      pDataBlock->nAllocSize = nAllocSizeOld;
      *numOfRows = (int32_t)(pDataBlock->nAllocSize - pDataBlock->headerSize) / rowSize;
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  *numOfRows = (int32_t)(pDataBlock->nAllocSize - pDataBlock->headerSize) / rowSize;
  return TSDB_CODE_SUCCESS;
}

static void tsSetBlockInfo(SSubmitBlk *pBlocks, const STableMeta *pTableMeta, int32_t numOfRows) {
  pBlocks->tid = pTableMeta->sid;
  pBlocks->uid = pTableMeta->uid;
  pBlocks->sversion = pTableMeta->sversion;
  pBlocks->numOfRows += numOfRows;
}

// data block is disordered, sort it in ascending order
void tscSortRemoveDataBlockDupRows(STableDataBlocks *dataBuf) {
  SSubmitBlk *pBlocks = (SSubmitBlk *)dataBuf->pData;

  // size is less than the total size, since duplicated rows may be removed yet.
  assert(pBlocks->numOfRows * dataBuf->rowSize + sizeof(SSubmitBlk) == dataBuf->size);

  // if use server time, this block must be ordered
  if (dataBuf->tsSource == TSDB_USE_SERVER_TS) {
    assert(dataBuf->ordered);
  }

  if (!dataBuf->ordered) {
    char *pBlockData = pBlocks->data;
    qsort(pBlockData, pBlocks->numOfRows, dataBuf->rowSize, rowDataCompar);

    int32_t i = 0;
    int32_t j = 1;

    while (j < pBlocks->numOfRows) {
      TSKEY ti = *(TSKEY *)(pBlockData + dataBuf->rowSize * i);
      TSKEY tj = *(TSKEY *)(pBlockData + dataBuf->rowSize * j);

      if (ti == tj) {
        ++j;
        continue;
      }

      int32_t nextPos = (++i);
      if (nextPos != j) {
        memmove(pBlockData + dataBuf->rowSize * nextPos, pBlockData + dataBuf->rowSize * j, dataBuf->rowSize);
      }

      ++j;
    }

    dataBuf->ordered = true;

    pBlocks->numOfRows = i + 1;
    dataBuf->size = sizeof(SSubmitBlk) + dataBuf->rowSize * pBlocks->numOfRows;
  }
}

static int32_t doParseInsertStatement(SSqlObj *pSql, void *pTableList, char **str, SParsedDataColInfo *spd,
                                      int32_t *totalNum) {
  SSqlCmd *       pCmd = &pSql->cmd;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableMeta *    pTableMeta = pTableMetaInfo->pTableMeta;
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  
  STableDataBlocks *dataBuf = NULL;
  int32_t ret = tscGetDataBlockFromList(pTableList, pCmd->pDataBlocks, pTableMeta->uid, TSDB_DEFAULT_PAYLOAD_SIZE,
                                        sizeof(SSubmitBlk), tinfo.rowSize, pTableMetaInfo->name,
                                        pTableMeta, &dataBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  
  int32_t maxNumOfRows;
  ret = tscAllocateMemIfNeed(dataBuf, tinfo.rowSize, &maxNumOfRows);
  if (TSDB_CODE_SUCCESS != ret) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_TSC_INVALID_SQL;
  char *  tmpTokenBuf = calloc(1, 4096);  // used for deleting Escape character: \\, \', \"
  if (NULL == tmpTokenBuf) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t numOfRows = tsParseValues(str, dataBuf, pTableMeta, maxNumOfRows, spd, pCmd->payload, &code, tmpTokenBuf);
  free(tmpTokenBuf);
  if (numOfRows <= 0) {
    return code;
  }

  for (uint32_t i = 0; i < dataBuf->numOfParams; ++i) {
    SParamInfo *param = dataBuf->params + i;
    if (param->idx == -1) {
      param->idx = pCmd->numOfParams++;
      param->offset -= sizeof(SSubmitBlk);
    }
  }

  SSubmitBlk *pBlocks = (SSubmitBlk *)(dataBuf->pData);
  tsSetBlockInfo(pBlocks, pTableMeta, numOfRows);

  dataBuf->vgId = pTableMeta->vgroupInfo.vgId;
  dataBuf->numOfTables = 1;

  /*
   * the value of pRes->numOfRows does not affect the true result of AFFECTED ROWS,
   * which is actually returned from server.
   */
  *totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t tscCheckIfCreateTable(char **sqlstr, SSqlObj *pSql) {
  int32_t   index = 0;
  SSQLToken sToken = {0};
  SSQLToken tableToken = {0};
  int32_t   code = TSDB_CODE_SUCCESS;
  
  const int32_t TABLE_INDEX = 0;
  const int32_t STABLE_INDEX = 1;
  
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  char *sql = *sqlstr;

  // get the token of specified table
  index = 0;
  tableToken = tStrGetToken(sql, &index, false, 0, NULL);
  sql += index;

  char *cstart = NULL;
  char *cend = NULL;

  // skip possibly exists column list
  index = 0;
  sToken = tStrGetToken(sql, &index, false, 0, NULL);
  sql += index;

  int32_t numOfColList = 0;
  bool    createTable = false;

  if (sToken.type == TK_LP) {
    cstart = &sToken.z[0];
    index = 0;
    while (1) {
      sToken = tStrGetToken(sql, &index, false, 0, NULL);
      if (sToken.type == TK_RP) {
        cend = &sToken.z[0];
        break;
      }

      ++numOfColList;
    }

    sToken = tStrGetToken(sql, &index, false, 0, NULL);
    sql += index;
  }

  if (numOfColList == 0 && cstart != NULL) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }
  
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
  
  if (sToken.type == TK_USING) {  // create table if not exists according to the super table
    index = 0;
    sToken = tStrGetToken(sql, &index, false, 0, NULL);
    sql += index;

    tscAllocPayload(pCmd, sizeof(STagData));
    STagData *pTag = (STagData *) pCmd->payload;

    memset(pTag, 0, sizeof(STagData));
    
    //the source super table is moved to the secondary position of the pTableMetaInfo list
    if (pQueryInfo->numOfTables < 2) {
      tscAddEmptyMetaInfo(pQueryInfo);
    }

    STableMetaInfo *pSTableMeterMetaInfo = tscGetMetaInfo(pQueryInfo, STABLE_INDEX);
    code = tscSetTableFullName(pSTableMeterMetaInfo, &sToken, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tstrncpy(pTag->name, pSTableMeterMetaInfo->name, sizeof(pTag->name));
    code = tscGetTableMeta(pSql, pSTableMeterMetaInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (!UTIL_TABLE_IS_SUPER_TABLE(pSTableMeterMetaInfo)) {
      return tscInvalidSQLErrMsg(pCmd->payload, "create table only from super table is allowed", sToken.z);
    }

    SSchema *pTagSchema = tscGetTableTagSchema(pSTableMeterMetaInfo->pTableMeta);
    STableComInfo tinfo = tscGetTableInfo(pSTableMeterMetaInfo->pTableMeta);
    
    index = 0;
    sToken = tStrGetToken(sql, &index, false, 0, NULL);
    sql += index;

    SParsedDataColInfo spd = {0};
    
    uint8_t numOfTags = tscGetNumOfTags(pSTableMeterMetaInfo->pTableMeta);
    spd.numOfCols = numOfTags;

    // if specify some tags column
    if (sToken.type != TK_LP) {
      tscSetAssignedColumnInfo(&spd, pTagSchema, numOfTags);
    } else {
      /* insert into tablename (col1, col2,..., coln) using superTableName (tagName1, tagName2, ..., tagNamen)
       * tags(tagVal1, tagVal2, ..., tagValn) values(v1, v2,... vn); */
      int16_t offset[TSDB_MAX_COLUMNS] = {0};
      for (int32_t t = 1; t < numOfTags; ++t) {
        offset[t] = offset[t - 1] + pTagSchema[t - 1].bytes;
      }

      while (1) {
        index = 0;
        sToken = tStrGetToken(sql, &index, false, 0, NULL);
        sql += index;

        if (TK_STRING == sToken.type) {
          strdequote(sToken.z);
          sToken.n = strtrim(sToken.z);
        }

        if (sToken.type == TK_RP) {
          break;
        }

        bool findColumnIndex = false;

        // todo speedup by using hash list
        for (int32_t t = 0; t < numOfTags; ++t) {
          if (strncmp(sToken.z, pTagSchema[t].name, sToken.n) == 0 && strlen(pTagSchema[t].name) == sToken.n) {
            SParsedColElem *pElem = &spd.elems[spd.numOfAssignedCols++];
            pElem->offset = offset[t];
            pElem->colIndex = t;

            if (spd.hasVal[t] == true) {
              return tscInvalidSQLErrMsg(pCmd->payload, "duplicated tag name", sToken.z);
            }

            spd.hasVal[t] = true;
            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {
          return tscInvalidSQLErrMsg(pCmd->payload, "invalid tag name", sToken.z);
        }
      }

      if (spd.numOfAssignedCols == 0 || spd.numOfAssignedCols > numOfTags) {
        return tscInvalidSQLErrMsg(pCmd->payload, "tag name expected", sToken.z);
      }

      index = 0;
      sToken = tStrGetToken(sql, &index, false, 0, NULL);
      sql += index;
    }

    if (sToken.type != TK_TAGS) {
      return tscInvalidSQLErrMsg(pCmd->payload, "keyword TAGS expected", sToken.z);
    }

    SKVRowBuilder kvRowBuilder = {0};
    if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    uint32_t ignoreTokenTypes = TK_LP;
    uint32_t numOfIgnoreToken = 1;
    for (int i = 0; i < spd.numOfAssignedCols; ++i) {
      SSchema* pSchema = pTagSchema + spd.elems[i].colIndex;

      index = 0;
      sToken = tStrGetToken(sql, &index, true, numOfIgnoreToken, &ignoreTokenTypes);
      sql += index;

      if (sToken.n == 0 || sToken.type == TK_RP) {
        break;
      }

      // Remove quotation marks
      if (TK_STRING == sToken.type) {
        sToken.z++;
        sToken.n -= 2;
      }

      char tagVal[TSDB_MAX_TAGS_LEN];
      code = tsParseOneColumnData(pSchema, &sToken, tagVal, pCmd->payload, &sql, false, tinfo.precision);
      if (code != TSDB_CODE_SUCCESS) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return code;
      }

      tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
    }

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    tdDestroyKVRowBuilder(&kvRowBuilder);
    if (row == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    tdSortKVRowByColIdx(row);
    pTag->dataLen = kvRowLen(row);
    kvRowCpy(pTag->data, row);
    free(row);

    index = 0;
    sToken = tStrGetToken(sql, &index, false, 0, NULL);
    sql += index;
    if (sToken.n == 0 || sToken.type != TK_RP) {
      return tscInvalidSQLErrMsg(pCmd->payload, ") expected", sToken.z);
    }

    pCmd->payloadLen = sizeof(pTag->name) + sizeof(pTag->dataLen) + pTag->dataLen;
    pTag->dataLen = htonl(pTag->dataLen);

    if (tscValidateName(&tableToken) != TSDB_CODE_SUCCESS) {
      return tscInvalidSQLErrMsg(pCmd->payload, "invalid table name", *sqlstr);
    }

    int32_t ret = tscSetTableFullName(pTableMetaInfo, &tableToken, pSql);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    createTable = true;
    code = tscGetMeterMetaEx(pSql, pTableMetaInfo, true);
    if (TSDB_CODE_TSC_ACTION_IN_PROGRESS == code) {
      return code;
    }
    
  } else {
    if (cstart != NULL) {
      sql = cstart;
    } else {
      sql = sToken.z;
    }
    code = tscGetMeterMetaEx(pSql, pTableMetaInfo, false);
    
    if (pCmd->curSql == NULL) {
      assert(code == TSDB_CODE_TSC_ACTION_IN_PROGRESS);
    }
  }

  int32_t len = cend - cstart + 1;
  if (cstart != NULL && createTable == true) {
    /* move the column list to start position of the next accessed points */
    memmove(sql - len, cstart, len);
    *sqlstr = sql - len;
  } else {
    *sqlstr = sql;
  }

  if (*sqlstr == NULL) {
    code = TSDB_CODE_TSC_INVALID_SQL;
  }
  
  return code;
}

int validateTableName(char *tblName, int len, SSQLToken* psTblToken) {
  tstrncpy(psTblToken->z, tblName, TSDB_TABLE_ID_LEN);

  psTblToken->n    = len;
  psTblToken->type = TK_ID;
  tSQLGetToken(psTblToken->z, &psTblToken->type);

  return tscValidateName(psTblToken);
}

static int32_t validateDataSource(SSqlCmd *pCmd, int8_t type, const char *sql) {
  if (pCmd->dataSourceType != 0 && pCmd->dataSourceType != type) {
    return tscInvalidSQLErrMsg(pCmd->payload, "keyword VALUES and FILE are not allowed to mix up", sql);
  }

  pCmd->dataSourceType = type;
  return TSDB_CODE_SUCCESS;
}

/**
 * usage: insert into table1 values() () table2 values()()
 *
 * @param str
 * @param acct
 * @param db
 * @param pSql
 * @return
 */
int tsParseInsertSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  char* str = pCmd->curSql;

  int32_t totalNum = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  assert(pQueryInfo != NULL);

  STableMetaInfo *pTableMetaInfo = NULL;
  if (pQueryInfo->numOfTables == 0) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  } else {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  }

  if ((code = tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (NULL == pCmd->pTableList) {
    pCmd->pTableList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
    pCmd->pDataBlocks = taosArrayInit(4, POINTER_BYTES);
    if (NULL == pCmd->pTableList || NULL == pSql->cmd.pDataBlocks) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  } else {
    str = pCmd->curSql;
  }
  
  tscDebug("%p create data block list for submit data:%p, pTableList:%p", pSql, pCmd->pDataBlocks, pCmd->pTableList);

  while (1) {
    int32_t   index = 0;
    SSQLToken sToken = tStrGetToken(str, &index, false, 0, NULL);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      /*
       * if the data is from the data file, no data has been generated yet. So, there no data to
       * merge or submit, save the file path and parse the file in other routines.
       */
      if (pCmd->dataSourceType == DATA_FROM_DATA_FILE) {
        goto _clean;
      }

      /*
       * if no data has been generated during parsing the sql string, error msg will return
       * Otherwise, create the first submit block and submit to virtual node.
       */
      if (totalNum == 0) {
        code = TSDB_CODE_TSC_INVALID_SQL;
        goto _error;
      } else {
        break;
      }
    }

    pCmd->curSql = sToken.z;
    char buf[TSDB_TABLE_ID_LEN];
    SSQLToken sTblToken;
    sTblToken.z = buf;
    // Check if the table name available or not
    if (validateTableName(sToken.z, sToken.n, &sTblToken) != TSDB_CODE_SUCCESS) {
      code = tscInvalidSQLErrMsg(pCmd->payload, "table name invalid", sToken.z);
      goto _error;
    }

    if ((code = tscSetTableFullName(pTableMetaInfo, &sTblToken, pSql)) != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    if ((code = tscCheckIfCreateTable(&str, pSql)) != TSDB_CODE_SUCCESS) {
      /*
       * After retrieving the table meta from server, the sql string will be parsed from the paused position.
       * And during the getTableMetaCallback function, the sql string will be parsed from the paused position.
       */
      if (TSDB_CODE_TSC_ACTION_IN_PROGRESS == code) {
        return code;
      }
      
      tscError("%p async insert parse error, code:%s", pSql, tstrerror(code));
      pCmd->curSql = NULL;
      goto _error;
    }

    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      code = tscInvalidSQLErrMsg(pCmd->payload, "insert data into super table is not supported", NULL);
      goto _error;
    }

    index = 0;
    sToken = tStrGetToken(str, &index, false, 0, NULL);
    str += index;

    if (sToken.n == 0) {
      code = tscInvalidSQLErrMsg(pCmd->payload, "keyword VALUES or FILE required", sToken.z);
      goto _error;
    }
    
    STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
    
    if (sToken.type == TK_VALUES) {
      SParsedDataColInfo spd = {.numOfCols = tinfo.numOfColumns};
      
      SSchema *pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
      tscSetAssignedColumnInfo(&spd, pSchema, tinfo.numOfColumns);

      if (validateDataSource(pCmd, DATA_FROM_SQL_STRING, sToken.z) != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      /*
       * app here insert data in different vnodes, so we need to set the following
       * data in another submit procedure using async insert routines
       */
      code = doParseInsertStatement(pSql, pCmd->pTableList, &str, &spd, &totalNum);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    } else if (sToken.type == TK_FILE) {
      if (validateDataSource(pCmd, DATA_FROM_DATA_FILE, sToken.z) != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      index = 0;
      sToken = tStrGetToken(str, &index, false, 0, NULL);
      str += index;
      if (sToken.n == 0) {
        code = tscInvalidSQLErrMsg(pCmd->payload, "file path is required following keyword FILE", sToken.z);
        goto _error;
      }

      strncpy(pCmd->payload, sToken.z, sToken.n);
      strdequote(pCmd->payload);

      // todo refactor extract method
      wordexp_t full_path;
      if (wordexp(pCmd->payload, &full_path, 0) != 0) {
        code = tscInvalidSQLErrMsg(pCmd->payload, "invalid filename", sToken.z);
        goto _error;
      }

      tstrncpy(pCmd->payload, full_path.we_wordv[0], pCmd->allocSize);
      wordfree(&full_path);

    } else if (sToken.type == TK_LP) {
      /* insert into tablename(col1, col2,..., coln) values(v1, v2,... vn); */
      STableMeta *pTableMeta = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0)->pTableMeta;
      SSchema *   pSchema = tscGetTableSchema(pTableMeta);

      if (validateDataSource(pCmd, DATA_FROM_SQL_STRING, sToken.z) != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      SParsedDataColInfo spd = {0};
      spd.numOfCols = tinfo.numOfColumns;

      int16_t offset[TSDB_MAX_COLUMNS] = {0};
      for (int32_t t = 1; t < tinfo.numOfColumns; ++t) {
        offset[t] = offset[t - 1] + pSchema[t - 1].bytes;
      }

      while (1) {
        index = 0;
        sToken = tStrGetToken(str, &index, false, 0, NULL);
        str += index;

        if (TK_STRING == sToken.type) {
          strdequote(sToken.z);
          sToken.n = strtrim(sToken.z);
        }

        if (sToken.type == TK_RP) {
          break;
        }

        bool findColumnIndex = false;

        // todo speedup by using hash list
        for (int32_t t = 0; t < tinfo.numOfColumns; ++t) {
          if (strncmp(sToken.z, pSchema[t].name, sToken.n) == 0 && strlen(pSchema[t].name) == sToken.n) {
            SParsedColElem *pElem = &spd.elems[spd.numOfAssignedCols++];
            pElem->offset = offset[t];
            pElem->colIndex = t;

            if (spd.hasVal[t] == true) {
              code = tscInvalidSQLErrMsg(pCmd->payload, "duplicated column name", sToken.z);
              goto _error;
            }

            spd.hasVal[t] = true;
            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {
          code = tscInvalidSQLErrMsg(pCmd->payload, "invalid column name", sToken.z);
          goto _error;
        }
      }

      if (spd.numOfAssignedCols == 0 || spd.numOfAssignedCols > tinfo.numOfColumns) {
        code = tscInvalidSQLErrMsg(pCmd->payload, "column name expected", sToken.z);
        goto _error;
      }

      index = 0;
      sToken = tStrGetToken(str, &index, false, 0, NULL);
      str += index;

      if (sToken.type != TK_VALUES) {
        code = tscInvalidSQLErrMsg(pCmd->payload, "keyword VALUES is expected", sToken.z);
        goto _error;
      }

      code = doParseInsertStatement(pSql, pCmd->pTableList, &str, &spd, &totalNum);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    } else {
      code = tscInvalidSQLErrMsg(pCmd->payload, "keyword VALUES or FILE are required", sToken.z);
      goto _error;
    }
  }

  // we need to keep the data blocks if there are parameters in the sql
  if (pCmd->numOfParams > 0) {
    goto _clean;
  }

  if (taosArrayGetSize(pCmd->pDataBlocks) > 0) { // merge according to vgId
    if ((code = tscMergeTableDataBlocks(pSql, pCmd->pDataBlocks)) != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = TSDB_CODE_SUCCESS;
  goto _clean;

_error:
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

_clean:
  taosHashCleanup(pCmd->pTableList);
  pCmd->pTableList = NULL;
  
  pCmd->curSql    = NULL;
  pCmd->parseFinished  = 1;
  
  return code;
}

int tsInsertInitialCheck(SSqlObj *pSql) {
  if (!pSql->pTscObj->writeAuth) {
    return TSDB_CODE_TSC_NO_WRITE_AUTH;
  }

  int32_t  index = 0;
  SSqlCmd *pCmd = &pSql->cmd;

  SSQLToken sToken = tStrGetToken(pSql->sqlstr, &index, false, 0, NULL);
  assert(sToken.type == TK_INSERT || sToken.type == TK_IMPORT);

  pCmd->count = 0;
  pCmd->command = TSDB_SQL_INSERT;
  pSql->res.numOfRows = 0;

  SQueryInfo *pQueryInfo = NULL;
  tscGetQueryInfoDetailSafely(pCmd, pCmd->clauseIndex, &pQueryInfo);

  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT | pCmd->insertType);

  sToken = tStrGetToken(pSql->sqlstr, &index, false, 0, NULL);
  if (sToken.type != TK_INTO) {
    return tscInvalidSQLErrMsg(pCmd->payload, "keyword INTO is expected", sToken.z);
  }

  pCmd->curSql = sToken.z + sToken.n;
  return TSDB_CODE_SUCCESS;
}

int tsParseSql(SSqlObj *pSql, bool initial) {
  int32_t ret = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  if ((!pCmd->parseFinished) && (!initial)) {
    tscDebug("%p resume to parse sql: %s", pSql, pCmd->curSql);
  }
  
  ret = tscAllocPayload(&pSql->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }

  if (tscIsInsertData(pSql->sqlstr)) {
    /*
     * Set the fp before parse the sql string, in case of getTableMeta failed, in which
     * the error handle callback function can rightfully restore the user-defined callback function (fp).
     */
    if (initial && (pSql->cmd.insertType != TSDB_QUERY_TYPE_STMT_INSERT)) {
      pSql->fetchFp = pSql->fp;
      pSql->fp = (void(*)())tscHandleMultivnodeInsert;
    }
    
    if (initial && ((ret = tsInsertInitialCheck(pSql)) != TSDB_CODE_SUCCESS)) {
      return ret;
    }
    
    ret = tsParseInsertSql(pSql);
  } else {
    SSqlInfo SQLInfo = qSQLParse(pSql->sqlstr);
    ret = tscToSQLCmd(pSql, &SQLInfo);
    SQLInfoDestroy(&SQLInfo);
  }

  /*
   * the pRes->code may be modified or released by another thread in tscTableMetaCallBack function,
   * so do NOT use pRes->code to determine if the getTableMeta function
   * invokes new threads to get data from mgmt node or simply retrieves data from cache.
   * do NOT assign return code to pRes->code for the same reason since it may be released by another thread already.
   */
  return ret;
}

static int doPackSendDataBlock(SSqlObj *pSql, int32_t numOfRows, STableDataBlocks *pTableDataBlocks) {
  int32_t  code = TSDB_CODE_SUCCESS;
  SSqlCmd *pCmd = &pSql->cmd;
  pSql->res.numOfRows = 0;

  assert(pCmd->numOfClause == 1);
  STableMeta *pTableMeta = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0)->pTableMeta;

  SSubmitBlk *pBlocks = (SSubmitBlk *)(pTableDataBlocks->pData);
  tsSetBlockInfo(pBlocks, pTableMeta, numOfRows);

  if ((code = tscMergeTableDataBlocks(pSql, pCmd->pDataBlocks)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableDataBlocks *pDataBlock = taosArrayGetP(pCmd->pDataBlocks, 0);
  if ((code = tscCopyDataBlockToPayload(pSql, pDataBlock)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  return tscProcessSql(pSql);
}

typedef struct SImportFileSupport {
  SSqlObj *pSql;
  FILE    *fp;
} SImportFileSupport;

static void parseFileSendDataBlock(void *param, TAOS_RES *tres, int code) {
  assert(param != NULL && tres != NULL);

  SSqlObj *pSql = tres;
  SSqlCmd *pCmd = &pSql->cmd;

  SImportFileSupport *pSupporter = (SImportFileSupport *) param;

  SSqlObj *pParentSql = pSupporter->pSql;
  FILE    *fp = pSupporter->fp;

  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {  // handle error
    assert(taos_errno(pSql) == code);

    taos_free_result(pSql);
    tfree(pSupporter);
    fclose(fp);

    pParentSql->res.code = code;
    tscQueueAsyncRes(pParentSql);
    return;
  }

  // accumulate the total submit records
  pParentSql->res.numOfRows += pSql->res.numOfRows;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableMeta *    pTableMeta = pTableMetaInfo->pTableMeta;
  SSchema *       pSchema = tscGetTableSchema(pTableMeta);
  STableComInfo   tinfo = tscGetTableInfo(pTableMeta);

  SParsedDataColInfo spd = {.numOfCols = tinfo.numOfColumns};
  tscSetAssignedColumnInfo(&spd, pSchema, tinfo.numOfColumns);

  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  int32_t count = 0;
  int32_t maxRows = 0;

  tscDestroyBlockArrayList(pSql->cmd.pDataBlocks);
  pCmd->pDataBlocks = taosArrayInit(1, POINTER_BYTES);

  STableDataBlocks *pTableDataBlock = NULL;
  int32_t ret = tscCreateDataBlock(TSDB_PAYLOAD_SIZE, tinfo.rowSize, sizeof(SSubmitBlk), pTableMetaInfo->name, pTableMeta, &pTableDataBlock);
  if (ret != TSDB_CODE_SUCCESS) {
//    return ret;
  }

  taosArrayPush(pCmd->pDataBlocks, &pTableDataBlock);
  tscAllocateMemIfNeed(pTableDataBlock, tinfo.rowSize, &maxRows);

  char *tokenBuf = calloc(1, 4096);

  while ((readLen = getline(&line, &n, fp)) != -1) {
    if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
      line[--readLen] = 0;
    }

    if (readLen == 0) {
      continue;
    }

    char *lineptr = line;
    strtolower(line, line);

    int32_t len =
        tsParseOneRowData(&lineptr, pTableDataBlock, pSchema, &spd, pCmd->payload, tinfo.precision, &code, tokenBuf);
    if (len <= 0 || pTableDataBlock->numOfParams > 0) {
      pSql->res.code = code;
      break;
    }

    pTableDataBlock->size += len;

    if (++count >= maxRows) {
      break;
    }
  }

  tfree(tokenBuf);
  free(line);

  if (count > 0) {
    code = doPackSendDataBlock(pSql, count, pTableDataBlock);
    if (code != TSDB_CODE_SUCCESS) {
      pParentSql->res.code = code;
      tscQueueAsyncRes(pParentSql);
      return;
    }

  } else {
    taos_free_result(pSql);
    tfree(pSupporter);
    fclose(fp);

    pParentSql->fp = pParentSql->fetchFp;

    // all data has been sent to vnode, call user function
    int32_t v = (pParentSql->res.code != TSDB_CODE_SUCCESS) ? pParentSql->res.code : pParentSql->res.numOfRows;
    (*pParentSql->fp)(pParentSql->param, pParentSql, v);
  }
}

void tscProcessMultiVnodesImportFromFile(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  if (pCmd->command != TSDB_SQL_INSERT) {
    return;
  }

  assert(pCmd->dataSourceType == DATA_FROM_DATA_FILE  && strlen(pCmd->payload) != 0);

  SImportFileSupport *pSupporter = calloc(1, sizeof(SImportFileSupport));
  SSqlObj *pNew = createSubqueryObj(pSql, 0, parseFileSendDataBlock, pSupporter, TSDB_SQL_INSERT, NULL);

  pNew->cmd.pDataBlocks = taosArrayInit(4, POINTER_BYTES);
  pCmd->count = 1;

  FILE *fp = fopen(pCmd->payload, "r");
  if (fp == NULL) {
    pSql->res.code = TAOS_SYSTEM_ERROR(errno);
    tscError("%p failed to open file %s to load data from file, code:%s", pSql, pCmd->payload, tstrerror(pSql->res.code));

    tfree(pSupporter)
    tscQueueAsyncRes(pSql);

    return;
  }

  pSupporter->pSql = pSql;
  pSupporter->fp = fp;

  parseFileSendDataBlock(pSupporter, pNew, 0);
}
