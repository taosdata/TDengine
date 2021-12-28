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

#include "dataBlockMgt.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"
#include "tglobal.h"
#include "ttime.h"
#include "ttoken.h"
#include "ttypes.h"

#define NEXT_TOKEN(pSql, sToken) \
  do { \
    int32_t index = 0; \
    sToken = tStrGetToken(pSql, &index, false); \
    pSql += index; \
  } while (0)

#define NEXT_TOKEN_KEEP_SQL(pSql, sToken, index) \
  do { \
    sToken = tStrGetToken(pSql, &index, false); \
  } while (0)

#define CHECK_CODE(expr) \
  do { \
    int32_t code = expr; \
    if (TSDB_CODE_SUCCESS != code) { \
      return code; \
    } \
  } while (0)

enum {
  TSDB_USE_SERVER_TS = 0,
  TSDB_USE_CLI_TS = 1,
};

typedef struct SInsertParseContext {
  SParseContext* pComCxt;       // input
  const char* pSql;             // input
  SMsgBuf msg;                  // input
  STableMeta* pTableMeta;       // each table
  SParsedDataColInfo tags;      // each table
  SKVRowBuilder tagsBuilder;    // each table
  SHashObj* pVgroupsHashObj;    // global
  SHashObj* pTableBlockHashObj; // global
  SArray* pTableDataBlocks;     // global
  SArray* pVgDataBlocks;        // global
  int32_t totalNum;
  SInsertStmtInfo* pOutput;
} SInsertParseContext;

typedef int32_t (*FRowAppend)(const void *value, int32_t len, void *param);

typedef struct SKvParam {
  char buf[TSDB_MAX_TAGS_LEN];
  SKVRowBuilder* builder;
  SSchema* schema;
} SKvParam;

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

static int32_t buildName(SInsertParseContext* pCxt, SToken* pStname, char* fullDbName, char* tableName) {
  if (parserValidateIdToken(pStname) != TSDB_CODE_SUCCESS) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid table name", pStname->z);
  }

  char* p = strnchr(pStname->z, TS_PATH_DELIMITER[0], pStname->n, false);
  if (NULL != p) { // db.table
    int32_t n = sprintf(fullDbName, "%d.", pCxt->pComCxt->ctx.acctId);
    strncpy(fullDbName + n, pStname->z, p - pStname->z);
    strncpy(tableName, p + 1, pStname->n - (p - pStname->z) - 1);
  } else {
    snprintf(fullDbName, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pComCxt->ctx.acctId, pCxt->pComCxt->ctx.db);
    strncpy(tableName, pStname->z, pStname->n);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getTableMeta(SInsertParseContext* pCxt, SToken* pTname) {
  char fullDbName[TSDB_DB_FNAME_LEN] = {0};
  char tableName[TSDB_TABLE_NAME_LEN] = {0};
  CHECK_CODE(buildName(pCxt, pTname, fullDbName, tableName));
  CHECK_CODE(catalogGetTableMeta(pCxt->pComCxt->ctx.pCatalog, pCxt->pComCxt->ctx.pTransporter, &pCxt->pComCxt->ctx.mgmtEpSet, fullDbName, tableName, &pCxt->pTableMeta));
  SVgroupInfo vg;
  CHECK_CODE(catalogGetTableHashVgroup(pCxt->pComCxt->ctx.pCatalog, pCxt->pComCxt->ctx.pTransporter, &pCxt->pComCxt->ctx.mgmtEpSet, fullDbName, tableName, &vg));
  CHECK_CODE(taosHashPut(pCxt->pVgroupsHashObj, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg)));
  return TSDB_CODE_SUCCESS;
}

static int32_t findCol(SToken* pColname, int32_t start, int32_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == pColname->n && strncmp(pColname->z, pSchema[start].name, pColname->n) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

static void buildMsgHeader(SVgDataBlocks* blocks) {
    SMsgDesc* desc = (SMsgDesc*)blocks->pData;
    desc->numOfVnodes = htonl(1);
    SSubmitMsg* submit = (SSubmitMsg*)(desc + 1);
    submit->header.vgId    = htonl(blocks->vg.vgId);
    submit->header.contLen = htonl(blocks->size - sizeof(SMsgDesc));
    submit->length         = submit->header.contLen;
    submit->numOfBlocks    = htonl(blocks->numOfTables);
    SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
    int32_t numOfBlocks = blocks->numOfTables;
    while (numOfBlocks--) {
      int32_t dataLen = blk->dataLen;
      blk->uid = htobe64(blk->uid);
      blk->tid = htonl(blk->tid);
      blk->padding = htonl(blk->padding);
      blk->sversion = htonl(blk->sversion);
      blk->dataLen = htonl(blk->dataLen);
      blk->schemaLen = htonl(blk->schemaLen);
      blk->numOfRows = htons(blk->numOfRows);
      blk = (SSubmitBlk*)(blk->data + dataLen);
    }
}

static int32_t buildOutput(SInsertParseContext* pCxt) {
  size_t numOfVg = taosArrayGetSize(pCxt->pVgDataBlocks);
  pCxt->pOutput->pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);
  if (NULL == pCxt->pOutput->pDataBlocks) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  for (size_t i = 0; i < numOfVg; ++i) {
    STableDataBlocks* src = taosArrayGetP(pCxt->pVgDataBlocks, i);
    SVgDataBlocks* dst = calloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosHashGetClone(pCxt->pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    dst->numOfTables = src->numOfTables;
    dst->size = src->size;
    SWAP(dst->pData, src->pData, char*);
    buildMsgHeader(dst);
    taosArrayPush(pCxt->pOutput->pDataBlocks, &dst);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTimestamp(STableDataBlocks *pDataBlocks, const char *start) {
  // once the data block is disordered, we do NOT keep previous timestamp any more
  if (!pDataBlocks->ordered) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY k = *(TSKEY *)start;

  if (k == INT64_MIN) {
    if (pDataBlocks->tsSource == TSDB_USE_CLI_TS) {
      return TSDB_CODE_FAILED; // client time/server time can not be mixed
    }
    pDataBlocks->tsSource = TSDB_USE_SERVER_TS;
  } else {
    if (pDataBlocks->tsSource == TSDB_USE_SERVER_TS) {
      return TSDB_CODE_FAILED;  // client time/server time can not be mixed
    }
    pDataBlocks->tsSource = TSDB_USE_CLI_TS;
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

static FORCE_INLINE int32_t KvRowAppend(const void *value, int32_t len, void *param) {
  SKvParam* pa = (SKvParam*)param;
  if (TSDB_DATA_TYPE_BINARY == pa->schema->type) {
    STR_WITH_SIZE_TO_VARSTR(pa->buf, value, len);
    tdAddColToKVRow(pa->builder, pa->schema->colId, pa->schema->type, pa->buf);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t output = 0;
    if (!taosMbsToUcs4(value, len, varDataVal(pa->buf), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }
    varDataSetLen(pa->buf, output);
    tdAddColToKVRow(pa->builder, pa->schema->colId, pa->schema->type, pa->buf);
  } else {
    tdAddColToKVRow(pa->builder, pa->schema->colId, pa->schema->type, value);
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct SMemParam {
  SMemRow row;
  SSchema* schema;
  int32_t toffset;
  uint8_t compareStat;
  int32_t dataLen;
  int32_t kvLen;
} SMemParam;

static FORCE_INLINE int32_t MemRowAppend(const void *value, int32_t len, void *param) {
  SMemParam* pa = (SMemParam*)param;
  if (TSDB_DATA_TYPE_BINARY == pa->schema->type) {
    char *rowEnd = memRowEnd(pa->row);
    STR_WITH_SIZE_TO_VARSTR(rowEnd, value, len);
    appendMemRowColValEx(pa->row, rowEnd, true, pa->schema->colId, pa->schema->type, pa->toffset, &pa->dataLen, &pa->kvLen, pa->compareStat);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t output = 0;
    char *  rowEnd = memRowEnd(pa->row);
    if (!taosMbsToUcs4(value, len, (char *)varDataVal(rowEnd), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }
    varDataSetLen(rowEnd, output);
    appendMemRowColValEx(pa->row, rowEnd, false, pa->schema->colId, pa->schema->type, pa->toffset, &pa->dataLen, &pa->kvLen, pa->compareStat);
  } else {
    appendMemRowColValEx(pa->row, value, true, pa->schema->colId, pa->schema->type, pa->toffset, &pa->dataLen, &pa->kvLen, pa->compareStat);
  }
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t checkAndTrimValue(SInsertParseContext* pCxt, SToken* pToken, SSchema* pSchema, char* tmpTokenBuf) {
  int16_t type = pToken->type;
  if ((type != TK_NOW && type != TK_INTEGER && type != TK_STRING && type != TK_FLOAT && type != TK_BOOL &&
        type != TK_NULL && type != TK_HEX && type != TK_OCT && type != TK_BIN) ||
      (pToken->n == 0) || (type == TK_RP)) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid data or symbol", pToken->z);
  }

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid numeric data", pToken->z);
  }

  // Remove quotation marks
  if (TK_STRING == type) {
    if (pToken->n >= TSDB_MAX_BYTES_PER_ROW) {
      return buildSyntaxErrMsg(&pCxt->msg, "too long string", pToken->z);
    }
    // delete escape character: \\, \', \"
    char delim = pToken->z[0];
    int32_t cnt = 0;
    int32_t j = 0;
    for (uint32_t k = 1; k < pToken->n - 1; ++k) {
      if (pToken->z[k] == '\\' || (pToken->z[k] == delim && pToken->z[k + 1] == delim)) {
        tmpTokenBuf[j] = pToken->z[k + 1];
        cnt++;
        j++;
        k++;
        continue;
      }
      tmpTokenBuf[j] = pToken->z[k];
      j++;
    }
    tmpTokenBuf[j] = 0;
    pToken->z = tmpTokenBuf;
    pToken->n -= 2 + cnt;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t parseOneValue(SInsertParseContext* pCxt, SToken* pToken, SSchema* pSchema, int16_t timePrec, char* tmpTokenBuf, FRowAppend func, void* param) {
  int64_t iv;
  int32_t ret;
  char *  endptr = NULL;

  CHECK_CODE(checkAndTrimValue(pCxt, pToken, pSchema, tmpTokenBuf));

  if (isNullStr(pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      int64_t tmpVal = 0;
      return func(&tmpVal, pSchema->bytes, param);
    }
    return func(getNullValue(pSchema->type), 0, param);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          return func(&TRUE_VALUE, pSchema->bytes, param);
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          return func(&FALSE_VALUE, pSchema->bytes, param);
        } else {
          return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_INTEGER) {
        return func(((strtoll(pToken->z, NULL, 10) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else if (pToken->type == TK_FLOAT) {
        return func(((strtod(pToken->z, NULL) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid bool data", pToken->z);
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, true)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "data overflow", pToken->z);
      }
      uint8_t tmpVal = (uint8_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_UTINYINT:{
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, false)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned tinyint data overflow", pToken->z);
      }
      uint8_t tmpVal = (uint8_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, true)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "smallint data overflow", pToken->z);
      }
      int16_t tmpVal = (int16_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, false)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned smallint data overflow", pToken->z);
      }
      uint16_t tmpVal = (uint16_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, true)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "int data overflow", pToken->z);
      }
      int32_t tmpVal = (int32_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, false)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned int data overflow", pToken->z);
      }
      uint32_t tmpVal = (uint32_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, true)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "bigint data overflow", pToken->z);
      }
      return func(&iv, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toInt64(pToken->z, pToken->type, pToken->n, &iv, false)) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "unsigned bigint data overflow", pToken->z);
      }
      uint64_t tmpVal = (uint64_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_FLOAT: {
      double dv;
      if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal float data", pToken->z);
      }
      float tmpVal = (float)dv;
      return func(&tmpVal, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double dv;
      if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(&pCxt->msg, "illegal double data", pToken->z);
      }
      return func(&dv, pSchema->bytes, param);
    }
    case TSDB_DATA_TYPE_BINARY: {
      // too long values will return invalid sql, not be truncated automatically
      if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {
        return buildSyntaxErrMsg(&pCxt->msg, "string data overflow", pToken->z);
      }
      return func(pToken->z, pToken->n, param);
    }
    case TSDB_DATA_TYPE_NCHAR: {
      return func(pToken->z, pToken->n, param);
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t tmpVal;
      if (parseTime(pCxt, pToken, timePrec, &tmpVal) != TSDB_CODE_SUCCESS) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid timestamp", pToken->z);
      }
      return func(&tmpVal, pSchema->bytes, param);
    }
  }

  return TSDB_CODE_FAILED;
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
static int32_t parseTagsClause(SInsertParseContext* pCxt, SSchema* pTagsSchema, uint8_t precision) {
  if (tdInitKVRowBuilder(&pCxt->tagsBuilder) < 0) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SKvParam param = {.builder = &pCxt->tagsBuilder};
  SToken sToken;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  for (int i = 0; i < pCxt->tags.numOfBound; ++i) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    SSchema* pSchema = &pTagsSchema[pCxt->tags.boundedColumns[i]];
    param.schema = pSchema;
    CHECK_CODE(parseOneValue(pCxt, &sToken, pSchema, precision, tmpTokenBuf, KvRowAppend, &param));
  }

  SKVRow row = tdGetKVRowFromBuilder(&pCxt->tagsBuilder);
  if (NULL == row) {
    return buildInvalidOperationMsg(&pCxt->msg, "tag value expected");
  }
  tdSortKVRowByColIdx(row);

  // todo construct payload

  tfree(row);
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
  setBoundColumnInfo(&pCxt->tags, pTagsSchema, getNumOfTags(pCxt->pTableMeta));

  // pSql -> [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_LP == sToken.type) {
    CHECK_CODE(parseBoundColumns(pCxt, &pCxt->tags, pTagsSchema));
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
  CHECK_CODE(parseTagsClause(pCxt, pTagsSchema, getTableInfo(pCxt->pTableMeta).precision));

  return TSDB_CODE_SUCCESS;
}

static int parseOneRow(SInsertParseContext* pCxt, STableDataBlocks* pDataBlocks, int16_t timePrec, int32_t* len, char* tmpTokenBuf) {  
  SParsedDataColInfo* spd = &pDataBlocks->boundColumnInfo;
  SMemRowBuilder* pBuilder = &pDataBlocks->rowBuilder;
  char *row = pDataBlocks->pData + pDataBlocks->size;  // skip the SSubmitBlk header
  initSMemRow(row, pBuilder->memRowType, pDataBlocks, spd->numOfBound);

  bool isParseBindParam = false;
  SSchema* schema = getTableColumnSchema(pDataBlocks->pTableMeta);
  SMemParam param = {.row = row};
  SToken sToken = {0};
  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    SSchema *pSchema = &schema[spd->boundedColumns[i]]; 
    param.schema = pSchema;
    param.compareStat = pBuilder->compareStat;
    getMemRowAppendInfo(schema, pBuilder->memRowType, spd, i, &param.toffset);
    CHECK_CODE(parseOneValue(pCxt, &sToken, pSchema, timePrec, tmpTokenBuf, MemRowAppend, &param));

    if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
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
      convertMemRow(row, spd->allNullLen + TD_MEM_ROW_DATA_HEAD_SIZE, pBuilder->kvRowInitLen);
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
static int32_t parseValues(SInsertParseContext* pCxt, STableDataBlocks* pDataBlock, int maxRows, int32_t* numOfRows) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initMemRowBuilder(&pDataBlock->rowBuilder, 0, tinfo.numOfColumns, pDataBlock->boundColumnInfo.numOfBound, pDataBlock->boundColumnInfo.allNullLen));

  (*numOfRows) = 0;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  SToken sToken;
  while (1) {
    int32_t index = 0;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_LP != sToken.type) {
      break;
    }
    pCxt->pSql += index;

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
  CHECK_CODE(parseValues(pCxt, dataBuf, maxNumOfRows, &numOfRows));

  SSubmitBlk *pBlocks = (SSubmitBlk *)(dataBuf->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, dataBuf->pTableMeta, numOfRows)) {
    return buildInvalidOperationMsg(&pCxt->msg, "too many rows in sql, total number of rows should be less than 32767");
  }

  dataBuf->numOfTables = 1;
  pCxt->totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static void destroyInsertParseContextForTable(SInsertParseContext* pCxt) {
  tfree(pCxt->pTableMeta);
  destroyBoundColumnInfo(&pCxt->tags);
  tdDestroyKVRowBuilder(&pCxt->tagsBuilder);
}

static void destroyInsertParseContext(SInsertParseContext* pCxt) {
  destroyInsertParseContextForTable(pCxt);
  taosHashCleanup(pCxt->pVgroupsHashObj);
  taosHashCleanup(pCxt->pTableBlockHashObj);
  destroyBlockArrayList(pCxt->pTableDataBlocks);
  destroyBlockArrayList(pCxt->pVgDataBlocks);
}

//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
static int32_t parseInsertBody(SInsertParseContext* pCxt) {
  // for each table
  while (1) {
    destroyInsertParseContextForTable(pCxt);

    SToken sToken;
    // pSql -> tb_name ...
    NEXT_TOKEN(pCxt->pSql, sToken);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      if (0 == pCxt->totalNum) {
        return buildInvalidOperationMsg(&pCxt->msg, "no data in sql");;
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
      CHECK_CODE(getTableMeta(pCxt, &tbnameToken));
    }

    STableDataBlocks *dataBuf = NULL;
    CHECK_CODE(getDataBlockFromList(pCxt->pTableBlockHashObj, pCxt->pTableMeta->uid, TSDB_DEFAULT_PAYLOAD_SIZE,
        sizeof(SSubmitBlk), getTableInfo(pCxt->pTableMeta).rowSize, pCxt->pTableMeta, &dataBuf, NULL));

    if (TK_LP == sToken.type) {
      // pSql -> field1_name, ...)
      CHECK_CODE(parseBoundColumns(pCxt, &dataBuf->boundColumnInfo, getTableColumnSchema(pCxt->pTableMeta)));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_VALUES == sToken.type) {
      // pSql -> (field1_value, ...) [(field1_value2, ...) ...]
      CHECK_CODE(parseValuesClause(pCxt, dataBuf));
      pCxt->pOutput->insertType = TSDB_QUERY_TYPE_INSERT;
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
      pCxt->pOutput->insertType = TSDB_QUERY_TYPE_FILE_INSERT;
      continue;
    }

    return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", sToken.z);
  }
  // merge according to vgId
  if (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT) && taosHashGetSize(pCxt->pTableBlockHashObj) > 0) {
    CHECK_CODE(mergeTableDataBlocks(pCxt->pTableBlockHashObj, pCxt->pOutput->schemaAttache, pCxt->pOutput->payloadType, &pCxt->pVgDataBlocks));
  }
  return buildOutput(pCxt);
}

// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
int32_t parseInsertSql(SParseContext* pContext, SInsertStmtInfo** pInfo) {
  SInsertParseContext context = {
    .pComCxt = pContext,
    .pSql = pContext->pSql,
    .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
    .pTableMeta = NULL,
    .pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false),
    .pTableBlockHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false),
    .totalNum = 0,
    .pOutput = calloc(1, sizeof(SInsertStmtInfo))
  };

  if (NULL == context.pVgroupsHashObj || NULL == context.pTableBlockHashObj || NULL == context.pOutput) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  *pInfo = context.pOutput;
  context.pOutput->nodeType = TSDB_SQL_INSERT;
  context.pOutput->schemaAttache = pContext->schemaAttached;
  context.pOutput->payloadType = PAYLOAD_TYPE_KV;

  int32_t code = skipInsertInto(&context);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBody(&context);
  }
  destroyInsertParseContext(&context);
  terrno = code;
  return (TSDB_CODE_SUCCESS == code ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED);
}
