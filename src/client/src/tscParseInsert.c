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

#include "ttype.h"
#include "hash.h"
#include "tscUtil.h"
#include "qTableMeta.h"
#include "tsclient.h"
#include "ttokendef.h"
#include "taosdef.h"

#include "tscLog.h"
#include "ttoken.h"

#include "tdataformat.h"

enum {
  TSDB_USE_SERVER_TS = 0,
  TSDB_USE_CLI_TS = 1,
};

static int32_t tscAllocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t *numOfRows);
static int32_t parseBoundColumns(SInsertStatementParam *pInsertParam, SParsedDataColInfo *pColInfo, SSchema *pSchema,
                                 char *str, char **end);
int            initMemRowBuilder(SMemRowBuilder *pBuilder, uint32_t nRows, uint32_t nCols, uint32_t nBoundCols,
                                 int32_t allNullLen) {
  ASSERT(nRows >= 0 && nCols > 0 && (nBoundCols <= nCols));
  if (nRows > 0) {
    // already init(bind multiple rows by single column)
    if (pBuilder->compareStat == ROW_COMPARE_NEED && (pBuilder->rowInfo != NULL)) {
      return TSDB_CODE_SUCCESS;
    }
  }

  if (nBoundCols == 0) {  // file input
    pBuilder->memRowType = SMEM_ROW_DATA;
    pBuilder->compareStat = ROW_COMPARE_NO_NEED;
    return TSDB_CODE_SUCCESS;
  } else {
    float boundRatio = ((float)nBoundCols / (float)nCols);

    if (boundRatio < KVRatioKV) {
      pBuilder->memRowType = SMEM_ROW_KV;
      pBuilder->compareStat = ROW_COMPARE_NO_NEED;
      return TSDB_CODE_SUCCESS;
    } else if (boundRatio > KVRatioData) {
      pBuilder->memRowType = SMEM_ROW_DATA;
      pBuilder->compareStat = ROW_COMPARE_NO_NEED;
      return TSDB_CODE_SUCCESS;
    }
    pBuilder->compareStat = ROW_COMPARE_NEED;

    if (boundRatio < KVRatioPredict) {
      pBuilder->memRowType = SMEM_ROW_KV;
    } else {
      pBuilder->memRowType = SMEM_ROW_DATA;
    }
  }

  pBuilder->dataRowInitLen = TD_MEM_ROW_DATA_HEAD_SIZE + allNullLen;
  pBuilder->kvRowInitLen = TD_MEM_ROW_KV_HEAD_SIZE + nBoundCols * sizeof(SColIdx);

  if (nRows > 0) {
    pBuilder->rowInfo = tcalloc(nRows, sizeof(SMemRowInfo));
    if (pBuilder->rowInfo == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    for (int i = 0; i < nRows; ++i) {
      (pBuilder->rowInfo + i)->dataLen = pBuilder->dataRowInitLen;
      (pBuilder->rowInfo + i)->kvLen = pBuilder->kvRowInitLen;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int tsParseTime(SStrToken *pToken, int64_t *time, char **next, char *error, int16_t timePrec) {
  int32_t   index = 0;
  SStrToken sToken;
  int64_t   interval;
  int64_t   useconds = 0;
  char *    pTokenEnd = *next;

  if (pToken->type == TK_NOW) {
    useconds = taosGetTimestamp(timePrec);
  } else if (strncmp(pToken->z, "0", 1) == 0 && pToken->n == 1) {
    // do nothing
  } else if (pToken->type == TK_INTEGER) {
    useconds = taosStr2int64(pToken->z);
  } else {
    // strptime("2001-11-12 18:31:01", "%Y-%m-%d %H:%M:%S", &tm);
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return tscInvalidOperationMsg(error, "invalid timestamp format", pToken->z);
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
  SStrToken valueToken;
  index = 0;
  sToken = tStrGetToken(pTokenEnd, &index, false);
  pTokenEnd += index;

  if (sToken.type == TK_MINUS || sToken.type == TK_PLUS) {
    index = 0;
    valueToken = tStrGetToken(pTokenEnd, &index, false);
    pTokenEnd += index;

    if (valueToken.n < 2) {
      return tscInvalidOperationMsg(error, "value expected in timestamp", sToken.z);
    }

    char unit = 0;
    if (parseAbsoluteDuration(valueToken.z, valueToken.n, &interval, &unit, timePrec) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (sToken.type == TK_PLUS) {
      useconds += interval;
    } else {
      useconds = useconds - interval;
    }

    *next = pTokenEnd;
  }

  *time = useconds;
  return TSDB_CODE_SUCCESS;
}

int32_t tsParseOneColumn(SSchema *pSchema, SStrToken *pToken, char *payload, char *msg, char **str, bool primaryKey,
                         int16_t timePrec) {
  int64_t iv;
  int32_t ret;
  char   *endptr = NULL;

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return tscInvalidOperationMsg(msg, "invalid numeric data", pToken->z);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_BOOL_NULL;
      } else {
        if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
          if (strncmp(pToken->z, "true", pToken->n) == 0) {
            *(uint8_t *)payload = TSDB_TRUE;
          } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
            *(uint8_t *)payload = TSDB_FALSE;
          } else {
            return tscSQLSyntaxErrMsg(msg, "invalid bool data", pToken->z);
          }
        } else if (pToken->type == TK_INTEGER) {
          iv = strtoll(pToken->z, NULL, 10);
          *(uint8_t *)payload = (int8_t)((iv == 0) ? TSDB_FALSE : TSDB_TRUE);
        } else if (pToken->type == TK_FLOAT) {
          double dv = strtod(pToken->z, NULL);
          *(uint8_t *)payload = (int8_t)((dv == 0) ? TSDB_FALSE : TSDB_TRUE);
        } else {
          return tscInvalidOperationMsg(msg, "invalid bool data", pToken->z);
        }
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT:
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_TINYINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid tinyint data", pToken->z);
        } else if (!IS_VALID_TINYINT(iv)) {
          return tscInvalidOperationMsg(msg, "data overflow", pToken->z);
        }

        *((uint8_t *)payload) = (uint8_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_UTINYINT:
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_UTINYINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned tinyint data", pToken->z);
        } else if (!IS_VALID_UTINYINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned tinyint data overflow", pToken->z);
        }

        *((uint8_t *)payload) = (uint8_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_SMALLINT:
      if (isNullStr(pToken)) {
        *((int16_t *)payload) = TSDB_DATA_SMALLINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid smallint data", pToken->z);
        } else if (!IS_VALID_SMALLINT(iv)) {
          return tscInvalidOperationMsg(msg, "smallint data overflow", pToken->z);
        }

        *((int16_t *)payload) = (int16_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_USMALLINT:
      if (isNullStr(pToken)) {
        *((uint16_t *)payload) = TSDB_DATA_USMALLINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned smallint data", pToken->z);
        } else if (!IS_VALID_USMALLINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned smallint data overflow", pToken->z);
        }

        *((uint16_t *)payload) = (uint16_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_INT:
      if (isNullStr(pToken)) {
        *((int32_t *)payload) = TSDB_DATA_INT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid int data", pToken->z);
        } else if (!IS_VALID_INT(iv)) {
          return tscInvalidOperationMsg(msg, "int data overflow", pToken->z);
        }

        *((int32_t *)payload) = (int32_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_UINT:
      if (isNullStr(pToken)) {
        *((uint32_t *)payload) = TSDB_DATA_UINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned int data", pToken->z);
        } else if (!IS_VALID_UINT(iv)) {
          return tscInvalidOperationMsg(msg, "unsigned int data overflow", pToken->z);
        }

        *((uint32_t *)payload) = (uint32_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (isNullStr(pToken)) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid bigint data", pToken->z);
        } else if (!IS_VALID_BIGINT(iv)) {
          return tscInvalidOperationMsg(msg, "bigint data overflow", pToken->z);
        }

        *((int64_t *)payload) = iv;
      }
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      if (isNullStr(pToken)) {
        *((uint64_t *)payload) = TSDB_DATA_UBIGINT_NULL;
      } else {
        ret = tStrToInteger(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return tscInvalidOperationMsg(msg, "invalid unsigned bigint data", pToken->z);
        } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
          return tscInvalidOperationMsg(msg, "unsigned bigint data overflow", pToken->z);
        }

        *((uint64_t *)payload) = iv;
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (isNullStr(pToken)) {
        *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidOperationMsg(msg, "illegal float data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) || isnan(dv)) {
          return tscInvalidOperationMsg(msg, "illegal float data", pToken->z);
        }

//        *((float *)payload) = (float)dv;
        SET_FLOAT_VAL(payload, dv);
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (isNullStr(pToken)) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == tscToDouble(pToken, &dv, &endptr)) {
          return tscInvalidOperationMsg(msg, "illegal double data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
          return tscInvalidOperationMsg(msg, "illegal double data", pToken->z);
        }

        *((double *)payload) = dv;
      }
      break;

    case TSDB_DATA_TYPE_BINARY:
      // binary data cannot be null-terminated char string, otherwise the last char of the string is lost
      if (pToken->type == TK_NULL) {
        setVardataNull(payload, TSDB_DATA_TYPE_BINARY);
      } else { // too long values will return invalid sql, not be truncated automatically
        if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) { //todo refactor
          return tscInvalidOperationMsg(msg, "string data overflow", pToken->z);
        }
        
        STR_WITH_SIZE_TO_VARSTR(payload, pToken->z, pToken->n);
      }

      break;

    case TSDB_DATA_TYPE_NCHAR:
      if (pToken->type == TK_NULL) {
        setVardataNull(payload, TSDB_DATA_TYPE_NCHAR);
      } else {
        // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
        int32_t output = 0;
        if (!taosMbsToUcs4(pToken->z, pToken->n, varDataVal(payload), pSchema->bytes - VARSTR_HEADER_SIZE, &output)) {
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), "%s", strerror(errno));
          return tscInvalidOperationMsg(msg, buf, pToken->z);
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
          return tscInvalidOperationMsg(msg, "invalid timestamp", pToken->z);
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
int32_t tsCheckTimestamp(STableDataBlocks *pDataBlocks, const char *start) {
  // once the data block is disordered, we do NOT keep previous timestamp any more
  if (!pDataBlocks->ordered) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY k = *(TSKEY *)start;

  if (k == INT64_MIN) {
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
    tscWarn("NOT ordered input timestamp");
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

int tsParseOneRow(char **str, STableDataBlocks *pDataBlocks, int16_t timePrec, int32_t *len, char *tmpTokenBuf,
                  SInsertStatementParam *pInsertParam) {
  int32_t   index = 0;
  SStrToken sToken = {0};

  char *row = pDataBlocks->pData + pDataBlocks->size;  // skip the SSubmitBlk header

  SParsedDataColInfo *spd = &pDataBlocks->boundColumnInfo;
  STableMeta *        pTableMeta = pDataBlocks->pTableMeta;
  SSchema *           schema = tscGetTableSchema(pTableMeta);
  SMemRowBuilder *    pBuilder = &pDataBlocks->rowBuilder;
  int32_t             dataLen = pBuilder->dataRowInitLen;
  int32_t             kvLen = pBuilder->kvRowInitLen;
  bool                isParseBindParam = false;

  initSMemRow(row, pBuilder->memRowType, pDataBlocks, spd->numOfBound);

  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    // the start position in data block buffer of current value in sql
    int32_t colIndex = spd->boundedColumns[i];

    char *start = row + spd->cols[colIndex].offset;

    SSchema *pSchema = &schema[colIndex];  // get colId here

    index = 0;
    sToken = tStrGetToken(*str, &index, true);
    *str += index;

    if (sToken.type == TK_QUESTION) {
      if (!isParseBindParam) {
        isParseBindParam = true;
      }
      if (pInsertParam->insertType != TSDB_QUERY_TYPE_STMT_INSERT) {
        return tscSQLSyntaxErrMsg(pInsertParam->msg, "? only allowed in binding insertion", *str);
      }

      uint32_t offset = (uint32_t)(start - pDataBlocks->pData);
      if (tscAddParamToDataBlock(pDataBlocks, pSchema->type, (uint8_t)timePrec, pSchema->bytes, offset) != NULL) {
        continue;
      }

      strcpy(pInsertParam->msg, "client out of memory");
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    int16_t type = sToken.type;
    if ((type != TK_NOW && type != TK_INTEGER && type != TK_STRING && type != TK_FLOAT && type != TK_BOOL &&
         type != TK_NULL && type != TK_HEX && type != TK_OCT && type != TK_BIN) ||
        (sToken.n == 0) || (type == TK_RP)) {
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "invalid data or symbol", sToken.z);
    }

    // Remove quotation marks
    if (TK_STRING == sToken.type) {
      // delete escape character: \\, \', \"
      char delim = sToken.z[0];

      int32_t cnt = 0;
      int32_t j = 0;
      if (sToken.n >= TSDB_MAX_BYTES_PER_ROW) {
        return tscSQLSyntaxErrMsg(pInsertParam->msg, "too long string", sToken.z);
      }

      for (uint32_t k = 1; k < sToken.n - 1; ++k) {
        if (sToken.z[k] == '\\' || (sToken.z[k] == delim && sToken.z[k + 1] == delim)) {
          tmpTokenBuf[j] = sToken.z[k + 1];

          cnt++;
          j++;
          k++;
          continue;
        }

        tmpTokenBuf[j] = sToken.z[k];
        j++;
      }

      tmpTokenBuf[j] = 0;
      sToken.z = tmpTokenBuf;
      sToken.n -= 2 + cnt;
    }

    bool    isPrimaryKey = (colIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX);
    int32_t toffset = -1;
    int16_t colId = -1;
    tscGetMemRowAppendInfo(schema, pBuilder->memRowType, spd, i, &toffset, &colId);

    int32_t ret = tsParseOneColumnKV(pSchema, &sToken, row, pInsertParam->msg, str, isPrimaryKey, timePrec, toffset,
                                     colId, &dataLen, &kvLen, pBuilder->compareStat);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (isPrimaryKey) {
      TSKEY tsKey = memRowKey(row);
      if (tsCheckTimestamp(pDataBlocks, (const char *)&tsKey) != TSDB_CODE_SUCCESS) {
        tscInvalidOperationMsg(pInsertParam->msg, "client time/server time can not be mixed up", sToken.z);
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
    }
  }

  if (!isParseBindParam) {
    // 2. check and set convert flag
    if (pBuilder->compareStat == ROW_COMPARE_NEED) {
      checkAndConvertMemRow(row, dataLen, kvLen);
    }

    // 3. set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && isDataRow(row) && !isNeedConvertRow(row)) {
      SDataRow dataRow = memRowDataBody(row);
      for (int32_t i = 0; i < spd->numOfCols; ++i) {
        if (spd->cols[i].valStat == VAL_STAT_NONE) {
          tdAppendDataColVal(dataRow, getNullValue(schema[i].type), true, schema[i].type, spd->cols[i].toffset);
        }
      }
    }
  }

  *len = getExtendedRowSize(pDataBlocks);

  return TSDB_CODE_SUCCESS;
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
int32_t schemaIdxCompar(const void *lhs, const void *rhs) {
  uint16_t left = *(uint16_t *)lhs;
  uint16_t right = *(uint16_t *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int32_t boundIdxCompar(const void *lhs, const void *rhs) {
  uint16_t left = *(uint16_t *)POINTER_SHIFT(lhs, sizeof(uint16_t));
  uint16_t right = *(uint16_t *)POINTER_SHIFT(rhs, sizeof(uint16_t));

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int32_t tsParseValues(char **str, STableDataBlocks *pDataBlock, int maxRows, SInsertStatementParam *pInsertParam,
    int32_t* numOfRows, char *tmpTokenBuf) {
  int32_t index = 0;
  int32_t code = 0;

  (*numOfRows) = 0;

  SStrToken sToken;

  STableMeta* pTableMeta = pDataBlock->pTableMeta;
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  
  int32_t  precision = tinfo.precision;

  int32_t extendedRowSize = getExtendedRowSize(pDataBlock);

  if (TSDB_CODE_SUCCESS !=
      (code = initMemRowBuilder(&pDataBlock->rowBuilder, 0, tinfo.numOfColumns, pDataBlock->boundColumnInfo.numOfBound,
                                pDataBlock->boundColumnInfo.allNullLen))) {
    return code;
  }
  while (1) {
    index = 0;
    sToken = tStrGetToken(*str, &index, false);
    if (sToken.n == 0 || sToken.type != TK_LP) break;

    *str += index;
    if ((*numOfRows) >= maxRows || pDataBlock->size + extendedRowSize >= pDataBlock->nAllocSize) {
      int32_t tSize;
      code = tscAllocateMemIfNeed(pDataBlock, extendedRowSize, &tSize);
      if (code != TSDB_CODE_SUCCESS) {  //TODO pass the correct error code to client
        strcpy(pInsertParam->msg, "client out of memory");
        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }

      ASSERT(tSize >= maxRows);
      maxRows = tSize;
    }

    int32_t len = 0;
    code = tsParseOneRow(str, pDataBlock, precision, &len, tmpTokenBuf, pInsertParam);
    if (code != TSDB_CODE_SUCCESS) {  // error message has been set in tsParseOneRow, return directly
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }

    pDataBlock->size += len;

    index = 0;
    sToken = tStrGetToken(*str, &index, false);
    if (sToken.n == 0 || sToken.type != TK_RP) {
      return tscSQLSyntaxErrMsg(pInsertParam->msg, ") expected", *str);
    }
    
    *str += index;

    (*numOfRows)++;
  }

  if ((*numOfRows) <= 0) {
    strcpy(pInsertParam->msg, "no any data points");
    return  TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

void tscSetBoundColumnInfo(SParsedDataColInfo *pColInfo, SSchema *pSchema, int32_t numOfCols) {
  pColInfo->numOfCols = numOfCols;
  pColInfo->numOfBound = numOfCols;
  pColInfo->orderStatus = ORDER_STATUS_ORDERED;  // default is ORDERED for non-bound mode
  pColInfo->boundedColumns = calloc(pColInfo->numOfCols, sizeof(int32_t));
  pColInfo->cols = calloc(pColInfo->numOfCols, sizeof(SBoundColumn));
  pColInfo->colIdxInfo = NULL;
  pColInfo->flen = 0;
  pColInfo->allNullLen = 0;

  int32_t nVar = 0;
  for (int32_t i = 0; i < pColInfo->numOfCols; ++i) {
    uint8_t type = pSchema[i].type;
    if (i > 0) {
      pColInfo->cols[i].offset = pSchema[i - 1].bytes + pColInfo->cols[i - 1].offset;
      pColInfo->cols[i].toffset = pColInfo->flen;
    }
    pColInfo->flen += TYPE_BYTES[type];
    switch (type) {
      case TSDB_DATA_TYPE_BINARY:
        pColInfo->allNullLen += (VARSTR_HEADER_SIZE + CHAR_BYTES);
        ++nVar;
        break;
      case TSDB_DATA_TYPE_NCHAR:
        pColInfo->allNullLen += (VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE);
        ++nVar;
        break;
      default:
        break;
    }
    pColInfo->boundedColumns[i] = i;
  }
  pColInfo->allNullLen += pColInfo->flen;
  pColInfo->extendedVarLen = (uint16_t)(nVar * sizeof(VarDataOffsetT));
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

int32_t FORCE_INLINE tsSetBlockInfo(SSubmitBlk *pBlocks, const STableMeta *pTableMeta, int32_t numOfRows) {
  pBlocks->tid = pTableMeta->id.tid;
  pBlocks->uid = pTableMeta->id.uid;
  pBlocks->sversion = pTableMeta->sversion;

  if (pBlocks->numOfRows + numOfRows >= INT16_MAX) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else {
    pBlocks->numOfRows += numOfRows;
    return TSDB_CODE_SUCCESS;
  }
}

// data block is disordered, sort it in ascending order
void tscSortRemoveDataBlockDupRowsRaw(STableDataBlocks *dataBuf) {
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

  dataBuf->prevTS = INT64_MIN;
}

// data block is disordered, sort it in ascending order
int tscSortRemoveDataBlockDupRows(STableDataBlocks *dataBuf, SBlockKeyInfo *pBlkKeyInfo) {
  SSubmitBlk *pBlocks = (SSubmitBlk *)dataBuf->pData;
  int16_t     nRows = pBlocks->numOfRows;

  // size is less than the total size, since duplicated rows may be removed yet.

  // if use server time, this block must be ordered
  if (dataBuf->tsSource == TSDB_USE_SERVER_TS) {
    assert(dataBuf->ordered);
  }
  // allocate memory
  size_t nAlloc = nRows * sizeof(SBlockKeyTuple);
  if (pBlkKeyInfo->pKeyTuple == NULL || pBlkKeyInfo->maxBytesAlloc < nAlloc) {
    size_t nRealAlloc = nAlloc + 10 * sizeof(SBlockKeyTuple);
    char * tmp = trealloc(pBlkKeyInfo->pKeyTuple, nRealAlloc);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    pBlkKeyInfo->pKeyTuple = (SBlockKeyTuple *)tmp;
    pBlkKeyInfo->maxBytesAlloc = (int32_t)nRealAlloc;
  }
  memset(pBlkKeyInfo->pKeyTuple, 0, nAlloc);

  int32_t         extendedRowSize = getExtendedRowSize(dataBuf);
  SBlockKeyTuple *pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
  char *          pBlockData = pBlocks->data;
  int             n = 0;
  while (n < nRows) {
    pBlkKeyTuple->skey = memRowKey(pBlockData);
    pBlkKeyTuple->payloadAddr = pBlockData;

    // next loop
    pBlockData += extendedRowSize;
    ++pBlkKeyTuple;
    ++n;
  }

  if (!dataBuf->ordered) {
    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
    qsort(pBlkKeyTuple, nRows, sizeof(SBlockKeyTuple), rowDataCompar);

    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
    int32_t i = 0;
    int32_t j = 1;
    while (j < nRows) {
      TSKEY ti = (pBlkKeyTuple + i)->skey;
      TSKEY tj = (pBlkKeyTuple + j)->skey;

      if (ti == tj) {
        ++j;
        continue;
      }

      int32_t nextPos = (++i);
      if (nextPos != j) {
        memmove(pBlkKeyTuple + nextPos, pBlkKeyTuple + j, sizeof(SBlockKeyTuple));
      }
      ++j;
    }

    dataBuf->ordered = true;
    pBlocks->numOfRows = i + 1;
  }

  dataBuf->size = sizeof(SSubmitBlk) + pBlocks->numOfRows * extendedRowSize;
  dataBuf->prevTS = INT64_MIN;

  return 0;
}

static int32_t doParseInsertStatement(SInsertStatementParam *pInsertParam, char **str, STableDataBlocks* dataBuf, int32_t *totalNum) {  
  int32_t maxNumOfRows;
  int32_t code = tscAllocateMemIfNeed(dataBuf, getExtendedRowSize(dataBuf), &maxNumOfRows);
  if (TSDB_CODE_SUCCESS != code) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  code = TSDB_CODE_TSC_INVALID_OPERATION;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"

  int32_t numOfRows = 0;
  code = tsParseValues(str, dataBuf, maxNumOfRows, pInsertParam, &numOfRows, tmpTokenBuf);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (uint32_t i = 0; i < dataBuf->numOfParams; ++i) {
    SParamInfo *param = dataBuf->params + i;
    if (param->idx == -1) {
      param->idx = pInsertParam->numOfParams++;
      param->offset -= sizeof(SSubmitBlk);
    }
  }

  SSubmitBlk *pBlocks = (SSubmitBlk *)(dataBuf->pData);
  code = tsSetBlockInfo(pBlocks, dataBuf->pTableMeta, numOfRows);
  if (code != TSDB_CODE_SUCCESS) {
    tscInvalidOperationMsg(pInsertParam->msg, "too many rows in sql, total number of rows should be less than 32767", *str);
    return code;
  }

  dataBuf->numOfTables = 1;
  *totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t tscCheckIfCreateTable(char **sqlstr, SSqlObj *pSql, char** boundColumn) {
  int32_t   index = 0;
  SStrToken sToken = {0};
  SStrToken tableToken = {0};
  int32_t   code = TSDB_CODE_SUCCESS;

  const int32_t TABLE_INDEX = 0;
  const int32_t STABLE_INDEX = 1;
  
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  SInsertStatementParam* pInsertParam = &pCmd->insertParam;

  char *sql = *sqlstr;

  // get the token of specified table
  index = 0;
  tableToken = tStrGetToken(sql, &index, false);
  sql += index;

  // skip possibly exists column list
  index = 0;
  sToken = tStrGetToken(sql, &index, false);
  sql += index;

  int32_t numOfColList = 0;

  // Bind table columns list in string, skip it and continue
  if (sToken.type == TK_LP) {
    *boundColumn = &sToken.z[0];

    while (1) {
      index = 0;
      sToken = tStrGetToken(sql, &index, false);

      if (sToken.type == TK_ILLEGAL) {
        return tscSQLSyntaxErrMsg(pCmd->payload, "unrecognized token", sToken.z);
      }
      
      if (sToken.type == TK_RP) {
        break;
      }

      sql += index;
      ++numOfColList;
    }

    sToken = tStrGetToken(sql, &index, false);
    sql += index;
  }

  if (numOfColList == 0 && (*boundColumn) != NULL) {
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }
  
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
  
  if (sToken.type == TK_USING) {  // create table if not exists according to the super table
    index = 0;
    sToken = tStrGetToken(sql, &index, false);
    sql += index;

    //the source super table is moved to the secondary position of the pTableMetaInfo list
    if (pQueryInfo->numOfTables < 2) {
      tscAddEmptyMetaInfo(pQueryInfo);
    }

    STableMetaInfo *pSTableMetaInfo = tscGetMetaInfo(pQueryInfo, STABLE_INDEX);
    code = tscSetTableFullName(&pSTableMetaInfo->name, &sToken, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tNameExtractFullName(&pSTableMetaInfo->name, pInsertParam->tagData.name);
    pInsertParam->tagData.dataLen = 0;

    code = tscGetTableMeta(pSql, pSTableMetaInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (!UTIL_TABLE_IS_SUPER_TABLE(pSTableMetaInfo)) {
      return tscInvalidOperationMsg(pInsertParam->msg, "create table only from super table is allowed", sToken.z);
    }

    SSchema *pTagSchema = tscGetTableTagSchema(pSTableMetaInfo->pTableMeta);
    STableComInfo tinfo = tscGetTableInfo(pSTableMetaInfo->pTableMeta);
    
    SParsedDataColInfo spd = {0};
    tscSetBoundColumnInfo(&spd, pTagSchema, tscGetNumOfTags(pSTableMetaInfo->pTableMeta));

    index = 0;
    sToken = tStrGetToken(sql, &index, false);
    if (sToken.type != TK_TAGS && sToken.type != TK_LP) {
      tscDestroyBoundColumnInfo(&spd);
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "keyword TAGS expected", sToken.z);
    }

    // parse the bound tags column
    if (sToken.type == TK_LP) {
      /*
       * insert into tablename (col1, col2,..., coln) using superTableName (tagName1, tagName2, ..., tagNamen)
       * tags(tagVal1, tagVal2, ..., tagValn) values(v1, v2,... vn);
       */
      char* end = NULL;
      code = parseBoundColumns(pInsertParam, &spd, pTagSchema, sql, &end);
      if (code != TSDB_CODE_SUCCESS) {
        tscDestroyBoundColumnInfo(&spd);
        return code;
      }

      sql = end;

      index = 0;  // keywords of "TAGS"
      sToken = tStrGetToken(sql, &index, false);
      sql += index;
    } else {
      sql += index;
    }

    index = 0;
    sToken = tStrGetToken(sql, &index, false);
    sql += index;

    if (sToken.type != TK_LP) {
      tscDestroyBoundColumnInfo(&spd);
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "( is expected", sToken.z);
    }
    
    SKVRowBuilder kvRowBuilder = {0};
    if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
      tscDestroyBoundColumnInfo(&spd);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    for (int i = 0; i < spd.numOfBound; ++i) {
      SSchema* pSchema = &pTagSchema[spd.boundedColumns[i]];

      index = 0;
      sToken = tStrGetToken(sql, &index, true);
      sql += index;

      if (TK_ILLEGAL == sToken.type) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        tscDestroyBoundColumnInfo(&spd);
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      }

      if (sToken.n == 0 || sToken.type == TK_RP) {
        break;
      }

      // Remove quotation marks
      if (TK_STRING == sToken.type) {
        sToken.z++;
        sToken.n -= 2;
      }

      char tagVal[TSDB_MAX_TAGS_LEN];
      code = tsParseOneColumn(pSchema, &sToken, tagVal, pInsertParam->msg, &sql, false, tinfo.precision);
      if (code != TSDB_CODE_SUCCESS) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        tscDestroyBoundColumnInfo(&spd);
        return code;
      }

      tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
    }

    tscDestroyBoundColumnInfo(&spd);

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    tdDestroyKVRowBuilder(&kvRowBuilder);
    if (row == NULL) {
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "tag value expected", NULL);
    }
    tdSortKVRowByColIdx(row);

    pInsertParam->tagData.dataLen = kvRowLen(row);
    if (pInsertParam->tagData.dataLen <= 0){
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "tag value expected", NULL);
    }
    
    char* pTag = realloc(pInsertParam->tagData.data, pInsertParam->tagData.dataLen);
    if (pTag == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    kvRowCpy(pTag, row);
    free(row);
    pInsertParam->tagData.data = pTag;

    index = 0;
    sToken = tStrGetToken(sql, &index, false);
    sql += index;
    if (sToken.n == 0 || sToken.type != TK_RP) {
      return tscSQLSyntaxErrMsg(pInsertParam->msg, ") expected", sToken.z);
    }

    /* parse columns after super table tags values.
     * insert into table_name using super_table(tag_name1, tag_name2) tags(tag_val1, tag_val2)
     * (normal_col1, normal_col2) values(normal_col1_val, normal_col2_val);
     * */
    index = 0;
    sToken = tStrGetToken(sql, &index, false);
    sql += index;
    int numOfColsAfterTags = 0;
    if (sToken.type == TK_LP) {
      if (*boundColumn != NULL) {
        return tscSQLSyntaxErrMsg(pInsertParam->msg, "bind columns again", sToken.z);
      } else {
        *boundColumn = &sToken.z[0];
      }

      while (1) {
        index = 0;
        sToken = tStrGetToken(sql, &index, false);

        if (sToken.type == TK_RP) {
          break;
        }

        if (sToken.n == 0 || sToken.type == TK_SEMI || index == 0) {
          return tscSQLSyntaxErrMsg(pCmd->payload, "unexpected token", sql);
        }

        sql += index;
        ++numOfColsAfterTags;
      }

      if (numOfColsAfterTags == 0 && (*boundColumn) != NULL) {
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      }

      sToken = tStrGetToken(sql, &index, false);
    }

    sql = sToken.z;

    if (tscValidateName(&tableToken) != TSDB_CODE_SUCCESS) {
      return tscInvalidOperationMsg(pInsertParam->msg, "invalid table name", *sqlstr);
    }

    int32_t ret = tscSetTableFullName(&pTableMetaInfo->name, &tableToken, pSql);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (sql == NULL) {
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }

    code = tscGetTableMetaEx(pSql, pTableMetaInfo, true, false);
    if (TSDB_CODE_TSC_ACTION_IN_PROGRESS == code) {
      return code;
    }
    
  } else {
    if (sToken.z == NULL) {
      return tscSQLSyntaxErrMsg(pInsertParam->msg, "", sql);
    }

    sql = sToken.z;
    code = tscGetTableMetaEx(pSql, pTableMetaInfo, false, false);
    if (pInsertParam->sql == NULL) {
      assert(code == TSDB_CODE_TSC_ACTION_IN_PROGRESS);
    }
  }

  *sqlstr = sql;
  return code;
}

int validateTableName(char *tblName, int len, SStrToken* psTblToken) {
  tstrncpy(psTblToken->z, tblName, TSDB_TABLE_FNAME_LEN);

  psTblToken->n    = len;
  psTblToken->type = TK_ID;
  tGetToken(psTblToken->z, &psTblToken->type);

  return tscValidateName(psTblToken);
}

static int32_t validateDataSource(SInsertStatementParam *pInsertParam, int32_t type, const char *sql) {
  uint32_t *insertType = &pInsertParam->insertType;
  if (*insertType == TSDB_QUERY_TYPE_STMT_INSERT && type == TSDB_QUERY_TYPE_INSERT) {
    return TSDB_CODE_SUCCESS;
  }

  if ((*insertType) != 0 && (*insertType) != type) {
    return tscSQLSyntaxErrMsg(pInsertParam->msg, "keyword VALUES and FILE are not allowed to mixed up", sql);
  }

  *insertType = type;
  return TSDB_CODE_SUCCESS;
}

static int32_t parseBoundColumns(SInsertStatementParam *pInsertParam, SParsedDataColInfo* pColInfo, SSchema* pSchema,
    char* str, char **end) {
  int32_t nCols = pColInfo->numOfCols;

  pColInfo->numOfBound = 0;
  memset(pColInfo->boundedColumns, 0, sizeof(int32_t) * nCols);
  for (int32_t i = 0; i < nCols; ++i) {
    pColInfo->cols[i].valStat = VAL_STAT_NONE;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t index = 0;
  SStrToken sToken = tStrGetToken(str, &index, false);
  str += index;

  if (sToken.type != TK_LP) {
    code = tscSQLSyntaxErrMsg(pInsertParam->msg, "( is expected", sToken.z);
    goto _clean;
  }

  bool    isOrdered = true;
  int32_t lastColIdx = -1;  // last column found
  while (1) {
    index = 0;
    sToken = tStrGetToken(str, &index, false);
    str += index;

    if (TK_STRING == sToken.type) {
      tscDequoteAndTrimToken(&sToken);
    }

    if (sToken.type == TK_RP) {
      if (end != NULL) {  // set the end position
        *end = str;
      }

      break;
    }

    bool findColumnIndex = false;

    // todo speedup by using hash list
    int32_t nScanned = 0, t = lastColIdx + 1;
    while (t < nCols) {
      if (strncmp(sToken.z, pSchema[t].name, sToken.n) == 0 && strlen(pSchema[t].name) == sToken.n) {
        if (pColInfo->cols[t].valStat == VAL_STAT_HAS) {
          code = tscInvalidOperationMsg(pInsertParam->msg, "duplicated column name", sToken.z);
          goto _clean;
        }

        pColInfo->cols[t].valStat = VAL_STAT_HAS;
        pColInfo->boundedColumns[pColInfo->numOfBound] = t;
        ++pColInfo->numOfBound;
        findColumnIndex = true;
        if (isOrdered && (lastColIdx > t)) {
          isOrdered = false;
        }
        lastColIdx = t;
        break;
      }
      ++t;
      ++nScanned;
    }
    if (!findColumnIndex) {
      t = 0;
      int32_t nRemain = nCols - nScanned;
      while (t < nRemain) {
        if (strncmp(sToken.z, pSchema[t].name, sToken.n) == 0 && strlen(pSchema[t].name) == sToken.n) {
          if (pColInfo->cols[t].valStat == VAL_STAT_HAS) {
            code = tscInvalidOperationMsg(pInsertParam->msg, "duplicated column name", sToken.z);
            goto _clean;
          }

          pColInfo->cols[t].valStat = VAL_STAT_HAS;
          pColInfo->boundedColumns[pColInfo->numOfBound] = t;
          ++pColInfo->numOfBound;
          findColumnIndex = true;
          if (isOrdered && (lastColIdx > t)) {
            isOrdered = false;
          }
          lastColIdx = t;
          break;
        }
        ++t;
      }
    }

    if (!findColumnIndex) {
      code = tscInvalidOperationMsg(pInsertParam->msg, "invalid column/tag name", sToken.z);
      goto _clean;
    }
  }

  pColInfo->orderStatus = isOrdered ? ORDER_STATUS_ORDERED : ORDER_STATUS_DISORDERED;

  if (!isOrdered) {
    pColInfo->colIdxInfo = tcalloc(pColInfo->numOfBound, sizeof(SBoundIdxInfo));
    if (pColInfo->colIdxInfo == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _clean;
    }
    SBoundIdxInfo *pColIdx = pColInfo->colIdxInfo;
    for (uint16_t i = 0; i < pColInfo->numOfBound; ++i) {
      pColIdx[i].schemaColIdx = (uint16_t)pColInfo->boundedColumns[i];
      pColIdx[i].boundIdx = i;
    }
    qsort(pColIdx, pColInfo->numOfBound, sizeof(SBoundIdxInfo), schemaIdxCompar);
    for (uint16_t i = 0; i < pColInfo->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    qsort(pColIdx, pColInfo->numOfBound, sizeof(SBoundIdxInfo), boundIdxCompar);
  }

  memset(&pColInfo->boundedColumns[pColInfo->numOfBound], 0,
         sizeof(int32_t) * (pColInfo->numOfCols - pColInfo->numOfBound));

  return TSDB_CODE_SUCCESS;

_clean:
  pInsertParam->sql = NULL;
  return code;
}

static int32_t getFileFullPath(SStrToken* pToken, char* output) {
  char path[PATH_MAX] = {0};
  strncpy(path, pToken->z, pToken->n);
  strdequote(path);

  wordexp_t full_path;
  if (wordexp(path, &full_path, 0) != 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  tstrncpy(output, full_path.we_wordv[0], PATH_MAX);
  wordfree(&full_path);
  return TSDB_CODE_SUCCESS;
}

/**
 * parse insert sql
 * @param pSql
 * @return
 */
int tsParseInsertSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  SInsertStatementParam* pInsertParam = &pCmd->insertParam;
  pInsertParam->objectId = pSql->self;
  char* str = pInsertParam->sql;

  int32_t totalNum = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  assert(pQueryInfo != NULL);

  STableMetaInfo *pTableMetaInfo = (pQueryInfo->numOfTables == 0)? tscAddEmptyMetaInfo(pQueryInfo):tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    code = terrno;
    return code;
  }

  if (NULL == pInsertParam->pTableBlockHashList) {
    pInsertParam->pTableBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    if (NULL == pInsertParam->pTableBlockHashList) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _clean;
    }
  } else {
    str = pInsertParam->sql;
  }
  
  tscDebug("0x%"PRIx64" create data block list hashList:%p", pSql->self, pInsertParam->pTableBlockHashList);

  while (1) {
    int32_t   index = 0;
    SStrToken sToken = tStrGetToken(str, &index, false);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      /*
       * if the data is from the data file, no data has been generated yet. So, there no data to
       * merge or submit, save the file path and parse the file in other routines.
       */
      if (TSDB_QUERY_HAS_TYPE(pInsertParam->insertType, TSDB_QUERY_TYPE_FILE_INSERT)) {
        goto _clean;
      }

      /*
       * if no data has been generated during parsing the sql string, error msg will return
       * Otherwise, create the first submit block and submit to virtual node.
       */
      if (totalNum == 0) {
        code = TSDB_CODE_TSC_INVALID_OPERATION;
        goto _clean;
      } else {
        break;
      }
    }

    pInsertParam->sql = sToken.z;
    char      buf[TSDB_TABLE_FNAME_LEN];
    SStrToken sTblToken;
    sTblToken.z = buf;
    // Check if the table name available or not
    if (validateTableName(sToken.z, sToken.n, &sTblToken) != TSDB_CODE_SUCCESS) {
      code = tscInvalidOperationMsg(pInsertParam->msg, "table name invalid", sToken.z);
      goto _clean;
    }

    if ((code = tscSetTableFullName(&pTableMetaInfo->name, &sTblToken, pSql)) != TSDB_CODE_SUCCESS) {
      goto _clean;
    }

    char *bindedColumns = NULL;
    if ((code = tscCheckIfCreateTable(&str, pSql, &bindedColumns)) != TSDB_CODE_SUCCESS) {
      /*
       * After retrieving the table meta from server, the sql string will be parsed from the paused position.
       * And during the getTableMetaCallback function, the sql string will be parsed from the paused position.
       */
      if (TSDB_CODE_TSC_ACTION_IN_PROGRESS == code) {
        return code;
      }

      tscError("0x%"PRIx64" async insert parse error, code:%s", pSql->self, tstrerror(code));
      pInsertParam->sql = NULL;
      goto _clean;
    }

    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      code = tscInvalidOperationMsg(pInsertParam->msg, "insert data into super table is not supported", NULL);
      goto _clean;
    }

    index = 0;
    sToken = tStrGetToken(str, &index, false);
    str += index;

    if (sToken.n == 0 || (sToken.type != TK_FILE && sToken.type != TK_VALUES)) {
      code = tscSQLSyntaxErrMsg(pInsertParam->msg, "keyword VALUES or FILE required", sToken.z);
      goto _clean;
    }

    STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
    if (sToken.type == TK_FILE) {
      if (validateDataSource(pInsertParam, TSDB_QUERY_TYPE_FILE_INSERT, sToken.z) != TSDB_CODE_SUCCESS) {
        goto _clean;
      }

      index = 0;
      sToken = tStrGetToken(str, &index, false);
      if (sToken.type != TK_STRING && sToken.type != TK_ID) {
        code = tscSQLSyntaxErrMsg(pInsertParam->msg, "file path is required following keyword FILE", sToken.z);
        goto _clean;
      }
      str += index;
      if (sToken.n == 0) {
        code = tscSQLSyntaxErrMsg(pInsertParam->msg, "file path is required following keyword FILE", sToken.z);
        goto _clean;
      }

      code = getFileFullPath(&sToken, pCmd->payload);
      if (code != TSDB_CODE_SUCCESS) {
        tscInvalidOperationMsg(pInsertParam->msg, "invalid filename", sToken.z);
        goto _clean;
      }
    } else {
      if (bindedColumns == NULL) {
        STableMeta *pTableMeta = pTableMetaInfo->pTableMeta;
        if (validateDataSource(pInsertParam, TSDB_QUERY_TYPE_INSERT, sToken.z) != TSDB_CODE_SUCCESS) {
          goto _clean;
        }

        STableDataBlocks *dataBuf = NULL;
        int32_t ret = tscGetDataBlockFromList(pInsertParam->pTableBlockHashList, pTableMeta->id.uid, TSDB_DEFAULT_PAYLOAD_SIZE,
                                              sizeof(SSubmitBlk), tinfo.rowSize, &pTableMetaInfo->name, pTableMeta,
                                              &dataBuf, NULL);
        if (ret != TSDB_CODE_SUCCESS) {
          goto _clean;
        }

        code = doParseInsertStatement(pInsertParam, &str, dataBuf, &totalNum);
        if (code != TSDB_CODE_SUCCESS) {
          goto _clean;
        }
      } else {  // bindedColumns != NULL
        // insert into tablename(col1, col2,..., coln) values(v1, v2,... vn);
        STableMeta *pTableMeta = tscGetTableMetaInfoFromCmd(pCmd, 0)->pTableMeta;

        if (validateDataSource(pInsertParam, TSDB_QUERY_TYPE_INSERT, sToken.z) != TSDB_CODE_SUCCESS) {
          goto _clean;
        }

        STableDataBlocks *dataBuf = NULL;
        int32_t ret = tscGetDataBlockFromList(pInsertParam->pTableBlockHashList, pTableMeta->id.uid, TSDB_DEFAULT_PAYLOAD_SIZE,
                                              sizeof(SSubmitBlk), tinfo.rowSize, &pTableMetaInfo->name, pTableMeta,
                                              &dataBuf, NULL);
        if (ret != TSDB_CODE_SUCCESS) {
          goto _clean;
        }

        SSchema *pSchema = tscGetTableSchema(pTableMeta);
        code = parseBoundColumns(pInsertParam, &dataBuf->boundColumnInfo, pSchema, bindedColumns, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          goto _clean;
        }

        if (dataBuf->boundColumnInfo.cols[0].valStat == VAL_STAT_NONE) {
          code = tscInvalidOperationMsg(pInsertParam->msg, "primary timestamp column can not be null", NULL);
          goto _clean;
        }

        if (sToken.type != TK_VALUES) {
          code = tscSQLSyntaxErrMsg(pInsertParam->msg, "keyword VALUES is expected", sToken.z);
          goto _clean;
        }

        code = doParseInsertStatement(pInsertParam, &str, dataBuf, &totalNum);
        if (code != TSDB_CODE_SUCCESS) {
          goto _clean;
        }
      }
    }
  }

  // we need to keep the data blocks if there are parameters in the sql
  if (pInsertParam->numOfParams > 0) {
    goto _clean;
  }

  // merge according to vgId
  if (!TSDB_QUERY_HAS_TYPE(pInsertParam->insertType, TSDB_QUERY_TYPE_STMT_INSERT) && taosHashGetSize(pInsertParam->pTableBlockHashList) > 0) {
    if ((code = tscMergeTableDataBlocks(pInsertParam, true)) != TSDB_CODE_SUCCESS) {
      goto _clean;
    }
  }

  code = TSDB_CODE_SUCCESS;
  goto _clean;

_clean:
  pInsertParam->sql = NULL;
  return code;
}

int tsInsertInitialCheck(SSqlObj *pSql) {
  if (!pSql->pTscObj->writeAuth) {
    return TSDB_CODE_TSC_NO_WRITE_AUTH;
  }

  int32_t  index = 0;
  SSqlCmd *pCmd = &pSql->cmd;

  SStrToken sToken = tStrGetToken(pSql->sqlstr, &index, false);
  assert(sToken.type == TK_INSERT || sToken.type == TK_IMPORT);

  pCmd->count   = 0;
  pCmd->command = TSDB_SQL_INSERT;
  SInsertStatementParam* pInsertParam = &pCmd->insertParam;

  SQueryInfo *pQueryInfo = tscGetQueryInfoS(pCmd);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT);

  sToken = tStrGetToken(pSql->sqlstr, &index, false);
  if (sToken.type != TK_INTO) {
    return tscSQLSyntaxErrMsg(pInsertParam->msg, "keyword INTO is expected", sToken.z);
  }

  pInsertParam->sql = sToken.z + sToken.n;
  return TSDB_CODE_SUCCESS;
}

int tsParseSql(SSqlObj *pSql, bool initial) {
  int32_t ret = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;
  if (!initial) {
    tscDebug("0x%"PRIx64" resume to parse sql: %s", pSql->self, pCmd->insertParam.sql);
  }

  ret = tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }

  if (tscIsInsertData(pSql->sqlstr)) {
    if (initial && ((ret = tsInsertInitialCheck(pSql)) != TSDB_CODE_SUCCESS)) {
      strncpy(pCmd->payload, pCmd->insertParam.msg, TSDB_DEFAULT_PAYLOAD_SIZE);
      return ret;
    }

    ret = tsParseInsertSql(pSql);
    if (pSql->parseRetry < 1 && (ret == TSDB_CODE_TSC_SQL_SYNTAX_ERROR || ret == TSDB_CODE_TSC_INVALID_OPERATION)) {
      tscDebug("0x%"PRIx64 " parse insert sql statement failed, code:%s, clear meta cache and retry ", pSql->self, tstrerror(ret));

      tscResetSqlCmd(pCmd, true, pSql->self);
      pSql->parseRetry++;

      if ((ret = tsInsertInitialCheck(pSql)) == TSDB_CODE_SUCCESS) {
        ret = tsParseInsertSql(pSql);
      }
    }

    if (ret != TSDB_CODE_SUCCESS) {
      strncpy(pCmd->payload, pCmd->insertParam.msg, TSDB_DEFAULT_PAYLOAD_SIZE);
    }
  } else {
    SSqlInfo sqlInfo = qSqlParse(pSql->sqlstr);
    ret = tscValidateSqlInfo(pSql, &sqlInfo);
    if (ret == TSDB_CODE_TSC_INVALID_OPERATION && pSql->parseRetry < 1 && sqlInfo.type == TSDB_SQL_SELECT) {
      tscDebug("0x%"PRIx64 " parse query sql statement failed, code:%s, clear meta cache and retry ", pSql->self, tstrerror(ret));

      tscResetSqlCmd(pCmd, true, pSql->self);
      pSql->parseRetry++;

      ret = tscValidateSqlInfo(pSql, &sqlInfo);
    }

    SqlInfoDestroy(&sqlInfo);
  }

  /*
   * the pRes->code may be modified or released by another thread in tscTableMetaCallBack function,
   * so do NOT use pRes->code to determine if the getTableMeta function
   * invokes new threads to get data from mgmt node or simply retrieves data from cache.
   * do NOT assign return code to pRes->code for the same reason since it may be released by another thread already.
   */
  return ret;
}

static int doPackSendDataBlock(SSqlObj* pSql, SInsertStatementParam *pInsertParam, STableMeta* pTableMeta, int32_t numOfRows, STableDataBlocks *pTableDataBlocks) {
  int32_t  code = TSDB_CODE_SUCCESS;

  SSubmitBlk *pBlocks = (SSubmitBlk *)(pTableDataBlocks->pData);
  code = tsSetBlockInfo(pBlocks, pTableMeta, numOfRows);
  if (code != TSDB_CODE_SUCCESS) {
    return tscInvalidOperationMsg(pInsertParam->msg, "too many rows in sql, total number of rows should be less than 32767", NULL);
  }

  if ((code = tscMergeTableDataBlocks(pInsertParam, true)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableDataBlocks *pDataBlock = taosArrayGetP(pInsertParam->pDataBlocks, 0);
  if ((code = tscCopyDataBlockToPayload(pSql, pDataBlock)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SImportFileSupport {
  SSqlObj *pSql;
  FILE    *fp;
} SImportFileSupport;

static void parseFileSendDataBlock(void *param, TAOS_RES *tres, int32_t numOfRows) {
  assert(param != NULL && tres != NULL);

  char *  tokenBuf = NULL;
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  int32_t count = 0;
  int32_t maxRows = 0;
  FILE *  fp   = NULL;

  SSqlObj *pSql = tres;
  SSqlCmd *pCmd = &pSql->cmd;

  SImportFileSupport *pSupporter = (SImportFileSupport *)param;

  SSqlObj *pParentSql = pSupporter->pSql;
  fp = pSupporter->fp;

  int32_t code = pSql->res.code;

  // retry parse data from file and import data from the begining again
  if (code == TSDB_CODE_TDB_TABLE_RECONFIGURE) {
    assert(pSql->res.numOfRows == 0);
    int32_t ret = fseek(fp, 0, SEEK_SET);
    if (ret < 0) {
      tscError("0x%"PRIx64" failed to seek SEEK_SET since:%s", pSql->self, tstrerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      goto _error;
    }
  } else if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  // accumulate the total submit records
  pParentSql->res.numOfRows += pSql->res.numOfRows;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);
  STableMeta *    pTableMeta = pTableMetaInfo->pTableMeta;
  STableComInfo   tinfo = tscGetTableInfo(pTableMeta);

  SInsertStatementParam* pInsertParam = &pCmd->insertParam;
  destroyTableNameList(pInsertParam);

  pInsertParam->pDataBlocks = tscDestroyBlockArrayList(pInsertParam->pDataBlocks);

  if (pInsertParam->pTableBlockHashList == NULL) {
    pInsertParam->pTableBlockHashList = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
    if (pInsertParam->pTableBlockHashList == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  STableDataBlocks *pTableDataBlock = NULL;
  int32_t ret = tscGetDataBlockFromList(pInsertParam->pTableBlockHashList, pTableMeta->id.uid, TSDB_PAYLOAD_SIZE,
                                        sizeof(SSubmitBlk), tinfo.rowSize, &pTableMetaInfo->name, pTableMeta,
                                        &pTableDataBlock, NULL);
  if (ret != TSDB_CODE_SUCCESS) {
    pParentSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  tscAllocateMemIfNeed(pTableDataBlock, getExtendedRowSize(pTableDataBlock), &maxRows);
  tokenBuf = calloc(1, TSDB_MAX_BYTES_PER_ROW);
  if (tokenBuf == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (TSDB_CODE_SUCCESS !=
      (ret = initMemRowBuilder(&pTableDataBlock->rowBuilder, 0, tinfo.numOfColumns, pTableDataBlock->numOfParams,
                               pTableDataBlock->boundColumnInfo.allNullLen))) {
    goto _error;
  }

  while ((readLen = tgetline(&line, &n, fp)) != -1) {
    if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
      line[--readLen] = 0;
    }

    if (readLen == 0) {
      continue;
    }

    char *lineptr = line;
    strtolower(line, line);

    int32_t len = 0;
    code = tsParseOneRow(&lineptr, pTableDataBlock, tinfo.precision, &len, tokenBuf, pInsertParam);
    if (code != TSDB_CODE_SUCCESS || pTableDataBlock->numOfParams > 0) {
      pSql->res.code = code;
      break;
    }

    pTableDataBlock->size += len;

    if (++count >= maxRows) {
      break;
    }
  }

  tfree(tokenBuf);
  tfree(line);

  pParentSql->res.code = code;
  if (code == TSDB_CODE_SUCCESS) {
    if (count > 0) {
      pSql->res.numOfRows = 0;
      code = doPackSendDataBlock(pSql, pInsertParam, pTableMeta, count, pTableDataBlock);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }

      tscBuildAndSendRequest(pSql, NULL);
      return;
    } else {
      taos_free_result(pSql);
      tfree(pSupporter);
      fclose(fp);

      pParentSql->fp = pParentSql->fetchFp;

      // all data has been sent to vnode, call user function
      int32_t v = (int32_t)pParentSql->res.numOfRows;
      (*pParentSql->fp)(pParentSql->param, pParentSql, v);
      return;
    }
  }

_error:
  tfree(tokenBuf);
  tfree(line);
  taos_free_result(pSql);
  tfree(pSupporter);
  fclose(fp);

  tscAsyncResultOnError(pParentSql);
}

void tscImportDataFromFile(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  if (pCmd->command != TSDB_SQL_INSERT) {
    return;
  }

  SInsertStatementParam* pInsertParam = &pCmd->insertParam;
  assert(TSDB_QUERY_HAS_TYPE(pInsertParam->insertType, TSDB_QUERY_TYPE_FILE_INSERT) && strlen(pCmd->payload) != 0);
  pCmd->active = pCmd->pQueryInfo;

  SImportFileSupport *pSupporter = calloc(1, sizeof(SImportFileSupport));
  SSqlObj *pNew = createSubqueryObj(pSql, 0, parseFileSendDataBlock, pSupporter, TSDB_SQL_INSERT, NULL);

  FILE *fp = fopen(pCmd->payload, "rb");
  if (fp == NULL) {
    pSql->res.code = TAOS_SYSTEM_ERROR(errno);
    tscError("0x%"PRIx64" failed to open file %s to load data from file, code:%s", pSql->self, pCmd->payload, tstrerror(pSql->res.code));

    tfree(pSupporter);
    taos_free_result(pNew);
    tscAsyncResultOnError(pSql);
    return;
  }

  pSupporter->pSql = pSql;
  pSupporter->fp   = fp;

  parseFileSendDataBlock(pSupporter, pNew, TSDB_CODE_SUCCESS);
}
