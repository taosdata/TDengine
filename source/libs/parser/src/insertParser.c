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

#include "insertParser.h"

// #include "astGenerator.h"
#include "dataBlockMgt.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"
#include "tglobal.h"
#include "ttime.h"
#include "ttoken.h"
// #include "function.h"
#include "ttypes.h"

#define NEXT_TOKEN(pSql, sToken) \
  do { \
    int32_t index = 0; \
    sToken = tStrGetToken(pSql, &index, false); \
    pSql += index; \
  } while (0)

#define CHECK_CODE(expr) \
  do { \
    int32_t code = expr; \
    if (TSDB_CODE_SUCCESS != code) { \
      terrno = code; \
      return terrno; \
    } \
  } while (0)

#define CHECK_CODE_1(expr, destroy) \
  do { \
    int32_t code = expr; \
    if (TSDB_CODE_SUCCESS != code) { \
      (void)destroy; \
      terrno = code; \
      return terrno; \
    } \
  } while (0)

#define CHECK_CODE_2(expr, destroy1, destroy2) \
  do { \
    int32_t code = expr; \
    if (TSDB_CODE_SUCCESS != code) { \
      (void)destroy1; \
      (void)destroy2; \
      terrno = code; \
      return terrno; \
    } \
  } while (0)

enum {
  TSDB_USE_SERVER_TS = 0,
  TSDB_USE_CLI_TS = 1,
};

typedef struct SInsertParseContext {
  SParseContext* pComCxt;
  const char* pSql;
  SMsgBuf msg;
  struct SCatalog* pCatalog;
  SMetaData meta;               // need release
  const STableMeta* pTableMeta;
  SHashObj* pTableBlockHashObj; // data block for each table. need release
  int32_t totalNum;
  SInsertStmtInfo* pOutput;
} SInsertParseContext;

static uint8_t TRUE_VALUE = (uint8_t)TSDB_TRUE;
static uint8_t FALSE_VALUE = (uint8_t)TSDB_FALSE;

static bool isNullStr(SToken *pToken) {
  return (pToken->type == TK_NULL) || ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                                       (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0));
}

static FORCE_INLINE int32_t toDouble(SToken *pToken, double *value, char **endPtr) {
  errno = 0;
  *value = strtold(pToken->z, endPtr);

  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_ILLEGAL;
  }

  return pToken->type;
}

static int32_t toInt64(const char* z, int16_t type, int32_t n, int64_t* value, bool issigned) {
  errno = 0;
  int32_t ret = 0;

  char* endPtr = NULL;
  if (type == TK_FLOAT) {
    double v = strtod(z, &endPtr);
    if ((errno == ERANGE && v == HUGE_VALF) || isinf(v) || isnan(v)) {
      ret = -1;
    } else if ((issigned && (v < INT64_MIN || v > INT64_MAX)) || ((!issigned) && (v < 0 || v > UINT64_MAX))) {
      ret = -1;
    } else {
      *value = (int64_t) round(v);
    }

    errno = 0;
    return ret;
  }

  int32_t radix = 10;
  if (type == TK_HEX) {
    radix = 16;
  } else if (type == TK_BIN) {
    radix = 2;
  }

  // the string may be overflow according to errno
  if (!issigned) {
    const char *p = z;
    while(*p != 0 && *p == ' ') p++;   
    if (*p != 0 && *p == '-') { return -1;}

    *value = strtoull(z, &endPtr, radix);
  } else {
    *value = strtoll(z, &endPtr, radix);
  }

  // not a valid integer number, return error
  if (endPtr - z != n || errno == ERANGE) {
    ret = -1;
  }

  errno = 0;
  return ret;
}

static int32_t createInsertStmtInfo(SInsertStmtInfo **pInsertInfo) {
  SInsertStmtInfo *info = calloc(1, sizeof(SQueryStmtInfo));
  if (NULL == info) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  // info->pTableBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  // if (NULL == info->pTableBlockHashList) {
  //   tfree(info);
  //   return TSDB_CODE_TSC_OUT_OF_MEMORY;
  // }
  *pInsertInfo = info;
  return TSDB_CODE_SUCCESS;
}

static int32_t skipInsertInto(SInsertParseContext* pCxt) {
  SToken sToken;
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_INSERT != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "keyword INSERT is expected", sToken.z);
  }
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_INTO != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "keyword INTO is expected", sToken.z);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildTableName(SInsertParseContext* pCxt, SToken* pStname, SArray* tableNameList) {
  if (parserValidateIdToken(pStname) != TSDB_CODE_SUCCESS) {
    return buildInvalidOperationMsg(&pCxt->msg, "invalid table name");
  }

  SName name = {0};
  strndequote(name.tname, pStname->z, pStname->n);
  taosArrayPush(tableNameList, &name);

  return TSDB_CODE_SUCCESS;
}

static int32_t buildMetaReq(SInsertParseContext* pCxt, SToken* pStname, SMetaReq* pMetaReq) {
  pMetaReq->pTableName = taosArrayInit(4, sizeof(SName));
  return buildTableName(pCxt, pStname, pMetaReq->pTableName);
}

static int32_t getTableMeta(SInsertParseContext* pCxt, SToken* pTname) {
  SMetaReq req;
  CHECK_CODE(buildMetaReq(pCxt, pTname, &req));
  CHECK_CODE(catalogGetMetaData(pCxt->pCatalog, &req, &pCxt->meta));
  pCxt->pTableMeta = (STableMeta*)taosArrayGetP(pCxt->meta.pTableMeta, 0);
  return TSDB_CODE_SUCCESS;
}

// todo speedup by using hash list
static int32_t findCol(SToken* pColname, int32_t start, int32_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == pColname->n && strncmp(pColname->z, pSchema[start].name, pColname->n) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

static int32_t checkTimestamp(STableDataBlocks *pDataBlocks, const char *start) {
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
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

static int parseTime(SInsertParseContext* pCxt, SToken *pToken, int16_t timePrec, int64_t *time) {
  int32_t   index = 0;
  SToken sToken;
  int64_t   interval;
  int64_t   useconds = 0;
  const char* pTokenEnd = pCxt->pSql;

  if (pToken->type == TK_NOW) {
    useconds = taosGetTimestamp(timePrec);
  } else if (strncmp(pToken->z, "0", 1) == 0 && pToken->n == 1) {
    // do nothing
  } else if (pToken->type == TK_INTEGER) {
    useconds = taosStr2int64(pToken->z);
  } else {
    // strptime("2001-11-12 18:31:01", "%Y-%m-%d %H:%M:%S", &tm);
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return buildSyntaxErrMsg(&pCxt->msg, "invalid timestamp format", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = pToken->n; pToken->z[k] != '\0'; k++) {
    if (pToken->z[k] == ' ' || pToken->z[k] == '\t') continue;
    if (pToken->z[k] == ',') {
      pCxt->pSql = pTokenEnd;
      *time = useconds;
      return 0;
    }

    break;
  }

  /*
   * time expression:
   * e.g., now+12a, now-5h
   */
  SToken valueToken;
  index = 0;
  sToken = tStrGetToken(pTokenEnd, &index, false);
  pTokenEnd += index;

  if (sToken.type == TK_MINUS || sToken.type == TK_PLUS) {
    index = 0;
    valueToken = tStrGetToken(pTokenEnd, &index, false);
    pTokenEnd += index;

    if (valueToken.n < 2) {
      return buildSyntaxErrMsg(&pCxt->msg, "value expected in timestamp", sToken.z);
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

    pCxt->pSql = pTokenEnd;
  }

  *time = useconds;
  return TSDB_CODE_SUCCESS;
}

static int32_t parseOneColumn(SInsertParseContext* pCxt, SToken* pToken, SSchema* pSchema, bool primaryKey, int16_t timePrec, char* payload) {
  int64_t iv;
  int32_t ret;
  char   *endptr = NULL;

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid numeric data", pToken->z);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_BOOL_NULL;
      } else {
        if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
          if (strncmp(pToken->z, "true", pToken->n) == 0) {
            *(uint8_t *)payload = TSDB_TRUE;
          } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
            *(uint8_t *)payload = TSDB_FALSE;
          } else {
            return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
          }
        } else if (pToken->type == TK_INTEGER) {
          iv = strtoll(pToken->z, NULL, 10);
          *(uint8_t *)payload = (int8_t)((iv == 0) ? TSDB_FALSE : TSDB_TRUE);
        } else if (pToken->type == TK_FLOAT) {
          double dv = strtod(pToken->z, NULL);
          *(uint8_t *)payload = (int8_t)((dv == 0) ? TSDB_FALSE : TSDB_TRUE);
        } else {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
        }
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT:
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_TINYINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid tinyint data", pToken->z);
        } else if (!IS_VALID_TINYINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "data overflow", pToken->z);
        }
        *((uint8_t *)payload) = (uint8_t)iv;
      }
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      if (isNullStr(pToken)) {
        *((uint8_t *)payload) = TSDB_DATA_UTINYINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned tinyint data", pToken->z);
        } else if (!IS_VALID_UTINYINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned tinyint data overflow", pToken->z);
        }
        *((uint8_t *)payload) = (uint8_t)iv;
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (isNullStr(pToken)) {
        *((int16_t *)payload) = TSDB_DATA_SMALLINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid smallint data", pToken->z);
        } else if (!IS_VALID_SMALLINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "smallint data overflow", pToken->z);
        }
        *((int16_t *)payload) = (int16_t)iv;
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      if (isNullStr(pToken)) {
        *((uint16_t *)payload) = TSDB_DATA_USMALLINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned smallint data", pToken->z);
        } else if (!IS_VALID_USMALLINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned smallint data overflow", pToken->z);
        }
        *((uint16_t *)payload) = (uint16_t)iv;
      }
      break;
    case TSDB_DATA_TYPE_INT:
      if (isNullStr(pToken)) {
        *((int32_t *)payload) = TSDB_DATA_INT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid int data", pToken->z);
        } else if (!IS_VALID_INT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "int data overflow", pToken->z);
        }

        *((int32_t *)payload) = (int32_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_UINT:
      if (isNullStr(pToken)) {
        *((uint32_t *)payload) = TSDB_DATA_UINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned int data", pToken->z);
        } else if (!IS_VALID_UINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned int data overflow", pToken->z);
        }

        *((uint32_t *)payload) = (uint32_t)iv;
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (isNullStr(pToken)) {
        *((int64_t *)payload) = TSDB_DATA_BIGINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bigint data", pToken->z);
        } else if (!IS_VALID_BIGINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "bigint data overflow", pToken->z);
        }

        *((int64_t *)payload) = iv;
      }
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      if (isNullStr(pToken)) {
        *((uint64_t *)payload) = TSDB_DATA_UBIGINT_NULL;
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned bigint data", pToken->z);
        } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned bigint data overflow", pToken->z);
        }

        *((uint64_t *)payload) = iv;
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (isNullStr(pToken)) {
        *((int32_t *)payload) = TSDB_DATA_FLOAT_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) || isnan(dv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
        }

        SET_FLOAT_VAL(payload, dv);
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (isNullStr(pToken)) {
        *((int64_t *)payload) = TSDB_DATA_DOUBLE_NULL;
      } else {
        double dv;
        if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
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
          return buildSyntaxErrMsg(&pCxt->msg, "string data overflow", pToken->z);
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
          return buildSyntaxErrMsg(&pCxt->msg, buf, pToken->z);
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
        if (parseTime(pCxt, pToken, timePrec, &temp) != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid timestamp", pToken->z);
        }
        
        *((int64_t *)payload) = temp;
      }

      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t parseOneColumnKV(SInsertParseContext* pCxt, SToken* pToken, SSchema* pSchema, bool primaryKey, int16_t timePrec,
    SMemRow row, int32_t toffset, int16_t colId, int32_t* dataLen, int32_t* kvLen, uint8_t compareStat) {
  int64_t iv;
  int32_t ret;
  char *  endptr = NULL;

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid numeric data", pToken->z);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {  // bool
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
          if (strncmp(pToken->z, "true", pToken->n) == 0) {
            appendMemRowColValEx(row, &TRUE_VALUE, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
          } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
            appendMemRowColValEx(row, &FALSE_VALUE, true, colId, pSchema->type, toffset, dataLen, kvLen,
                                    compareStat);
          } else {
            return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
          }
        } else if (pToken->type == TK_INTEGER) {
          iv = strtoll(pToken->z, NULL, 10);
          appendMemRowColValEx(row, ((iv == 0) ? &FALSE_VALUE : &TRUE_VALUE), true, colId, pSchema->type, toffset,
                                  dataLen, kvLen, compareStat);
        } else if (pToken->type == TK_FLOAT) {
          double dv = strtod(pToken->z, NULL);
          appendMemRowColValEx(row, ((dv == 0) ? &FALSE_VALUE : &TRUE_VALUE), true, colId, pSchema->type, toffset,
                                  dataLen, kvLen, compareStat);
        } else {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
        }
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid tinyint data", pToken->z);
        } else if (!IS_VALID_TINYINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "data overflow", pToken->z);
        }

        uint8_t tmpVal = (uint8_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_UTINYINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned tinyint data", pToken->z);
        } else if (!IS_VALID_UTINYINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned tinyint data overflow", pToken->z);
        }

        uint8_t tmpVal = (uint8_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_SMALLINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid smallint data", pToken->z);
        } else if (!IS_VALID_SMALLINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "smallint data overflow", pToken->z);
        }

        int16_t tmpVal = (int16_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_USMALLINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned smallint data", pToken->z);
        } else if (!IS_VALID_USMALLINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned smallint data overflow", pToken->z);
        }

        uint16_t tmpVal = (uint16_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_INT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid int data", pToken->z);
        } else if (!IS_VALID_INT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "int data overflow", pToken->z);
        }

        int32_t tmpVal = (int32_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_UINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned int data", pToken->z);
        } else if (!IS_VALID_UINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned int data overflow", pToken->z);
        }

        uint32_t tmpVal = (uint32_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;

    case TSDB_DATA_TYPE_BIGINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, true);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bigint data", pToken->z);
        } else if (!IS_VALID_BIGINT(iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "bigint data overflow", pToken->z);
        }

        appendMemRowColValEx(row, &iv, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        ret = toInt64(pToken->z, pToken->type, pToken->n, &iv, false);
        if (ret != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned bigint data", pToken->z);
        } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "unsigned bigint data overflow", pToken->z);
        }

        uint64_t tmpVal = (uint64_t)iv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_FLOAT:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        double dv;
        if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) ||
            isnan(dv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
        }

        float tmpVal = (float)dv;
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      if (isNullStr(pToken)) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        double dv;
        if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
        }

        if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
          return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
        }

        appendMemRowColValEx(row, &dv, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_BINARY:
      // binary data cannot be null-terminated char string, otherwise the last char of the string is lost
      if (pToken->type == TK_NULL) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {  // too long values will return invalid sql, not be truncated automatically
        if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {  // todo refactor
          return buildSyntaxErrMsg(&pCxt->msg, "string data overflow", pToken->z);
        }
        // STR_WITH_SIZE_TO_VARSTR(payload, pToken->z, pToken->n);
        char *rowEnd = memRowEnd(row);
        STR_WITH_SIZE_TO_VARSTR(rowEnd, pToken->z, pToken->n);
        appendMemRowColValEx(row, rowEnd, false, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_NCHAR:
      if (pToken->type == TK_NULL) {
        appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                compareStat);
      } else {
        // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
        int32_t output = 0;
        char *  rowEnd = memRowEnd(row);
        if (!taosMbsToUcs4(pToken->z, pToken->n, (char *)varDataVal(rowEnd), pSchema->bytes - VARSTR_HEADER_SIZE,
                           &output)) {
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), "%s", strerror(errno));
          return buildSyntaxErrMsg(&pCxt->msg, buf, pToken->z);
        }
        varDataSetLen(rowEnd, output);
        appendMemRowColValEx(row, rowEnd, false, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }
      break;

    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (pToken->type == TK_NULL) {
        if (primaryKey) {
          // When building SKVRow primaryKey, we should not skip even with NULL value.
          int64_t tmpVal = 0;
          appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
        } else {
          appendMemRowColValEx(row, getNullValue(pSchema->type), true, colId, pSchema->type, toffset, dataLen, kvLen,
                                  compareStat);
        }
      } else {
        int64_t tmpVal;
        if (parseTime(pCxt, pToken, timePrec, &tmpVal) != TSDB_CODE_SUCCESS) {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid timestamp", pToken->z);
        }
        appendMemRowColValEx(row, &tmpVal, true, colId, pSchema->type, toffset, dataLen, kvLen, compareStat);
      }

      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_name, ...)
static int32_t parseBoundColumns(SInsertParseContext* pCxt, SParsedDataColInfo* pColList, SSchema* pSchema) {
  int32_t nCols = pColList->numOfCols;

  pColList->numOfBound = 0; 
  memset(pColList->boundedColumns, 0, sizeof(int32_t) * nCols);
  for (int32_t i = 0; i < nCols; ++i) {
    pColList->cols[i].valStat = VAL_STAT_NONE;
  }

  SToken sToken;
  bool    isOrdered = true;
  int32_t lastColIdx = -1;  // last column found
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);

    if (TK_RP == sToken.type) {
      break;
    }

    int32_t t = lastColIdx + 1;
    int32_t index = findCol(&sToken, t, nCols, pSchema);
    if (index < 0 && t > 0) {
      index = findCol(&sToken, 0, t, pSchema);
      isOrdered = false;
    }
    if (index < 0) {
      return buildSyntaxErrMsg(&pCxt->msg, "invalid column/tag name", sToken.z);
    }
    if (pColList->cols[index].valStat == VAL_STAT_HAS) {
      return buildSyntaxErrMsg(&pCxt->msg, "duplicated column name", sToken.z);
    }
    lastColIdx = index;
    pColList->cols[index].valStat = VAL_STAT_HAS;
    pColList->boundedColumns[pColList->numOfBound] = index;
    ++pColList->numOfBound;
  }

  pColList->orderStatus = isOrdered ? ORDER_STATUS_ORDERED : ORDER_STATUS_DISORDERED;

  if (!isOrdered) {
    pColList->colIdxInfo = calloc(pColList->numOfBound, sizeof(SBoundIdxInfo));
    if (NULL == pColList->colIdxInfo) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    SBoundIdxInfo* pColIdx = pColList->colIdxInfo;
    for (uint16_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].schemaColIdx = (uint16_t)pColList->boundedColumns[i];
      pColIdx[i].boundIdx = i;
    }
    qsort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), schemaIdxCompar);
    for (uint16_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    qsort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), boundIdxCompar);
  }

  memset(&pColList->boundedColumns[pColList->numOfBound], 0, sizeof(int32_t) * (pColList->numOfCols - pColList->numOfBound));

  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_value, ...)
static int32_t parseTagsClause(SInsertParseContext* pCxt, SParsedDataColInfo* pSpd, SSchema* pTagsSchema, uint8_t precision) {
  SKVRowBuilder kvRowBuilder = {0};
  if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
    destroyBoundColumnInfo(pSpd);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SToken sToken;
  for (int i = 0; i < pSpd->numOfBound; ++i) {
    SSchema* pSchema = &pTagsSchema[pSpd->boundedColumns[i]];

    NEXT_TOKEN(pCxt->pSql, sToken);

    if (TK_ILLEGAL == sToken.type) {
      tdDestroyKVRowBuilder(&kvRowBuilder);
      destroyBoundColumnInfo(pSpd);
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
    CHECK_CODE_2(parseOneColumn(pCxt, &sToken, pSchema, false, precision, tagVal), tdDestroyKVRowBuilder(&kvRowBuilder), destroyBoundColumnInfo(pSpd));
    tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
  }
  
  destroyBoundColumnInfo(pSpd);

  SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
  tdDestroyKVRowBuilder(&kvRowBuilder);
  if (NULL == row) {
    return buildInvalidOperationMsg(&pCxt->msg, "tag value expected");
  }
  tdSortKVRowByColIdx(row);

  // pInsertParam->tagData.dataLen = kvRowLen(row);
  // if (pInsertParam->tagData.dataLen <= 0){
  //   return buildInvalidOperationMsg(msgBuf, "tag value expected");
  // }
  
  // char* pTag = realloc(pInsertParam->tagData.data, pInsertParam->tagData.dataLen);
  // if (pTag == NULL) {
  //   return TSDB_CODE_TSC_OUT_OF_MEMORY;
  // }

  // kvRowCpy(pTag, row);
  tfree(row);
  // pInsertParam->tagData.data = pTag;
}

// pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t parseUsingClause(SInsertParseContext* pCxt, SToken* pTbnameToken) {
  SToken sToken;

  // pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  CHECK_CODE(getTableMeta(pCxt, &sToken));
  if (TSDB_SUPER_TABLE != pCxt->pTableMeta->tableType) {
    return buildInvalidOperationMsg(&pCxt->msg, "create table only from super table is allowed");
  }

  SSchema* pTagsSchema = getTableTagSchema(pCxt->pTableMeta);
  SParsedDataColInfo spd = {0};
  setBoundColumnInfo(&spd, pTagsSchema, getNumOfTags(pCxt->pTableMeta));

  // pSql -> [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_LP == sToken.type) {
    CHECK_CODE_1(parseBoundColumns(pCxt, &spd, pTagsSchema), destroyBoundColumnInfo(&spd));
    NEXT_TOKEN(pCxt->pSql, sToken);
  }

  if (TK_TAGS != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "TAGS is expected", sToken.z);
  }
  // pSql -> (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_LP != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "( is expected", sToken.z);
  }
  CHECK_CODE(parseTagsClause(pCxt, &spd, pTagsSchema, getTableInfo(pCxt->pTableMeta).precision));

  return TSDB_CODE_SUCCESS;
}

static int parseOneRow(SInsertParseContext* pCxt, STableDataBlocks* pDataBlocks, int16_t timePrec, int32_t* len, char* tmpTokenBuf) {
  int32_t   index = 0;
  SToken sToken = {0};

  char *row = pDataBlocks->pData + pDataBlocks->size;  // skip the SSubmitBlk header

  SParsedDataColInfo *spd = &pDataBlocks->boundColumnInfo;
  STableMeta *        pTableMeta = pDataBlocks->pTableMeta;
  SSchema *           schema = getTableColumnSchema(pTableMeta);
  SMemRowBuilder *    pBuilder = &pDataBlocks->rowBuilder;
  int32_t             dataLen = spd->allNullLen + TD_MEM_ROW_DATA_HEAD_SIZE;
  int32_t             kvLen = pBuilder->kvRowInitLen;
  bool                isParseBindParam = false;

  initSMemRow(row, pBuilder->memRowType, pDataBlocks, spd->numOfBound);

  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    // the start position in data block buffer of current value in sql
    int32_t colIndex = spd->boundedColumns[i];

    char *start = row + spd->cols[colIndex].offset;

    SSchema *pSchema = &schema[colIndex];  // get colId here

    NEXT_TOKEN(pCxt->pSql, sToken);

    // if (sToken.type == TK_QUESTION) {
    //   if (!isParseBindParam) {
    //     isParseBindParam = true;
    //   }
    //   if (pInsertParam->insertType != TSDB_QUERY_TYPE_STMT_INSERT) {
    //     return buildSyntaxErrMsg(pInsertParam->msg, "? only allowed in binding insertion", *str);
    //   }

    //   uint32_t offset = (uint32_t)(start - pDataBlocks->pData);
    //   if (tscAddParamToDataBlock(pDataBlocks, pSchema->type, (uint8_t)timePrec, pSchema->bytes, offset) != NULL) {
    //     continue;
    //   }

    //   strcpy(pInsertParam->msg, "client out of memory");
    //   return TSDB_CODE_TSC_OUT_OF_MEMORY;
    // }

    int16_t type = sToken.type;
    if ((type != TK_NOW && type != TK_INTEGER && type != TK_STRING && type != TK_FLOAT && type != TK_BOOL &&
         type != TK_NULL && type != TK_HEX && type != TK_OCT && type != TK_BIN) ||
        (sToken.n == 0) || (type == TK_RP)) {
      return buildSyntaxErrMsg(&pCxt->msg, "invalid data or symbol", sToken.z);
    }

    // Remove quotation marks
    if (TK_STRING == sToken.type) {
      // delete escape character: \\, \', \"
      char delim = sToken.z[0];

      int32_t cnt = 0;
      int32_t j = 0;
      if (sToken.n >= TSDB_MAX_BYTES_PER_ROW) {
        return buildSyntaxErrMsg(&pCxt->msg, "too long string", sToken.z);
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

    bool    isPrimaryKey = (colIndex == PRIMARYKEY_TIMESTAMP_COL_ID);
    int32_t toffset = -1;
    int16_t colId = -1;
    getMemRowAppendInfo(schema, pBuilder->memRowType, spd, i, &toffset, &colId);

    int32_t ret = parseOneColumnKV(pCxt, &sToken, pSchema, isPrimaryKey, timePrec, row, toffset,
                                     colId, &dataLen, &kvLen, pBuilder->compareStat);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (isPrimaryKey) {
      TSKEY tsKey = memRowKey(row);
      if (checkTimestamp(pDataBlocks, (const char *)&tsKey) != TSDB_CODE_SUCCESS) {
        buildSyntaxErrMsg(&pCxt->msg, "client time/server time can not be mixed up", sToken.z);
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

// pSql -> (field1_value, ...) [(field1_value2, ...) ...]
static int32_t parseValues(SInsertParseContext* pCxt, STableDataBlocks* pDataBlock, int maxRows, int32_t* numOfRows, char* tmpTokenBuf) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initMemRowBuilder(&pDataBlock->rowBuilder, 0, tinfo.numOfColumns, pDataBlock->boundColumnInfo.numOfBound, pDataBlock->boundColumnInfo.allNullLen));

  (*numOfRows) = 0;
  SToken sToken;
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    if (TK_LP != sToken.type) {
      break;
    }

    if ((*numOfRows) >= maxRows || pDataBlock->size + extendedRowSize >= pDataBlock->nAllocSize) {
      int32_t tSize;
      CHECK_CODE(allocateMemIfNeed(pDataBlock, extendedRowSize, &tSize));
      ASSERT(tSize >= maxRows);
      maxRows = tSize;
    }

    int32_t len = 0;
    CHECK_CODE(parseOneRow(pCxt, pDataBlock, tinfo.precision, &len, tmpTokenBuf));
    pDataBlock->size += len;

    NEXT_TOKEN(pCxt->pSql, sToken);
    if (TK_RP != sToken.type) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", sToken.z);
    }

    (*numOfRows)++;
  }

  if (0 == (*numOfRows)) {
    return  buildSyntaxErrMsg(&pCxt->msg, "no any data points", NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t parseValuesClause(SInsertParseContext* pCxt, STableDataBlocks* dataBuf) {  
  int32_t maxNumOfRows;
  CHECK_CODE(allocateMemIfNeed(dataBuf, getExtendedRowSize(dataBuf), &maxNumOfRows));

  int32_t numOfRows = 0;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  CHECK_CODE(parseValues(pCxt, dataBuf, maxNumOfRows, &numOfRows, tmpTokenBuf));

  for (uint32_t i = 0; i < dataBuf->numOfParams; ++i) {
    SParamInfo *param = dataBuf->params + i;
    if (param->idx == -1) {
      // param->idx = pInsertParam->numOfParams++;
      param->offset -= sizeof(SSubmitBlk);
    }
  }

  SSubmitBlk *pBlocks = (SSubmitBlk *)(dataBuf->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, dataBuf->pTableMeta, numOfRows)) {
    return buildInvalidOperationMsg(&pCxt->msg, "too many rows in sql, total number of rows should be less than 32767");
  }

  dataBuf->numOfTables = 1;
  pCxt->totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
static int32_t parseInsertBody(SInsertParseContext* pCxt) {
  while (1) {
    SToken sToken;
    // pSql -> tb_name ...
    NEXT_TOKEN(pCxt->pSql, sToken);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      if (0 == pCxt->totalNum) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      break;
    }

    SToken tbnameToken = sToken;

    NEXT_TOKEN(pCxt->pSql, sToken);

    // USING cluase
    if (TK_USING == sToken.type) {
      CHECK_CODE(parseUsingClause(pCxt, &tbnameToken));
      NEXT_TOKEN(pCxt->pSql, sToken);
    } else {
      CHECK_CODE(getTableMeta(pCxt, &sToken));
    }

    STableDataBlocks *dataBuf = NULL;
    CHECK_CODE(getDataBlockFromList(pCxt->pTableBlockHashObj, pCxt->pTableMeta->uid, TSDB_DEFAULT_PAYLOAD_SIZE,
        sizeof(SSubmitBlk), getTableInfo(pCxt->pTableMeta).rowSize, NULL/* tbname */, pCxt->pTableMeta, &dataBuf, NULL));

    if (TK_LP == sToken.type) {
      // pSql -> field1_name, ...)
      NEXT_TOKEN(pCxt->pSql, sToken);
      // todo col_list
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (TK_RP != sToken.type) {
        return buildSyntaxErrMsg(&pCxt->msg, ") is expected", sToken.z);
      }
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_VALUES == sToken.type) {
      // pSql -> (field1_value, ...) [(field1_value2, ...) ...]
      CHECK_CODE(parseValuesClause(pCxt, dataBuf));
      continue;
    }

    // FILE csv_file_path
    if (TK_FILE == sToken.type) {
      // pSql -> csv_file_path
      NEXT_TOKEN(pCxt->pSql, sToken);

      if (0 == sToken.n || (TK_STRING != sToken.type && TK_ID != sToken.type)) {
        return buildSyntaxErrMsg(&pCxt->msg, "file path is required following keyword FILE", sToken.z);
      }

      // todo
      continue;
    }

    return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", sToken.z);
  }
  // merge according to vgId
  if (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT) && taosHashGetSize(pCxt->pTableBlockHashObj) > 0) {
    CHECK_CODE(mergeTableDataBlocks(pCxt->pTableBlockHashObj, pCxt->pOutput->schemaAttache, pCxt->pOutput->payloadType, true));
  }
  return TSDB_CODE_SUCCESS;
}

// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
int32_t parseInsertSql(SParseContext* pContext, SInsertStmtInfo** pInfo) {
  CHECK_CODE(createInsertStmtInfo(pInfo));

  SInsertParseContext context = {
    .pComCxt = pContext,
    .pSql = pContext->pSql,
    .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
    .pCatalog = getCatalogHandle(pContext->pEpSet),
    .pTableMeta = NULL,
    .pTableBlockHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false),
    .totalNum = 0,
    .pOutput = *pInfo
  };

  if (NULL == context.pTableBlockHashObj) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  CHECK_CODE(skipInsertInto(&context));
  CHECK_CODE(parseInsertBody(&context));

  return TSDB_CODE_SUCCESS;
}
