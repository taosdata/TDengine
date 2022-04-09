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

#include "parInsertData.h"
#include "parInt.h"
#include "parUtil.h"
#include "parToken.h"
#include "tglobal.h"
#include "ttime.h"
#include "ttypes.h"

#define NEXT_TOKEN(pSql, sToken) \
  do { \
    int32_t index = 0; \
    sToken = tStrGetToken(pSql, &index, false); \
    pSql += index; \
  } while (0)

#define NEXT_TOKEN_WITH_PREV(pSql, sToken) \
  do { \
    int32_t index = 0; \
    sToken = tStrGetToken(pSql, &index, true); \
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

typedef struct SInsertParseContext {
  SParseContext* pComCxt;       // input
  char          *pSql;          // input
  SMsgBuf        msg;           // input
  STableMeta* pTableMeta;       // each table
  SParsedDataColInfo tags;      // each table
  SKVRowBuilder tagsBuilder;    // each table
  SVCreateTbReq createTblReq;   // each table
  SHashObj* pVgroupsHashObj;    // global
  SHashObj* pTableBlockHashObj; // global
  SHashObj* pSubTableHashObj;   // global
  SArray* pTableDataBlocks;     // global
  SArray* pVgDataBlocks;        // global
  int32_t totalNum;
  SVnodeModifOpStmt* pOutput;
} SInsertParseContext;

typedef int32_t (*_row_append_fn_t)(SMsgBuf* pMsgBuf, const void *value, int32_t len, void *param);

static uint8_t TRUE_VALUE = (uint8_t)TSDB_TRUE;
static uint8_t FALSE_VALUE = (uint8_t)TSDB_FALSE;

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

static int32_t parserValidateIdToken(SToken* pToken) {
  if (pToken == NULL || pToken->z == NULL || pToken->type != TK_NK_ID) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  // it is a token quoted with escape char '`'
  if (pToken->z[0] == TS_ESCAPE_CHAR && pToken->z[pToken->n - 1] == TS_ESCAPE_CHAR) {
    return TSDB_CODE_SUCCESS;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // It is a single part token, not a complex type
    if (isNumber(pToken)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    strntolower(pToken->z, pToken->z, pToken->n);
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_NK_SPACE) {
      pToken->n = (uint32_t)strtrim(pToken->z);
    }

    pToken->n = tGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type != TK_NK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = (uint32_t)(oldLen - (sep - pStr) - 1);
    int32_t len = tGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || pToken->type != TK_NK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      uint32_t offset = (uint32_t)(pToken->z - (pStr + firstPartLen + 1));
      memset(pToken->z + pToken->n - offset, ' ', offset);
    }

    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;

    strntolower(pToken->z, pToken->z, pToken->n);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildName(SInsertParseContext* pCxt, SToken* pStname, char* fullDbName, char* tableName) {
  if (parserValidateIdToken(pStname) != TSDB_CODE_SUCCESS) {
    return buildSyntaxErrMsg(&pCxt->msg, "invalid table name", pStname->z);
  }

  char* p = strnchr(pStname->z, TS_PATH_DELIMITER[0], pStname->n, false);
  if (NULL != p) { // db.table
    int32_t n = sprintf(fullDbName, "%d.", pCxt->pComCxt->acctId);
    strncpy(fullDbName + n, pStname->z, p - pStname->z);
    strncpy(tableName, p + 1, pStname->n - (p - pStname->z) - 1);
  } else {
    snprintf(fullDbName, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pComCxt->acctId, pCxt->pComCxt->db);
    strncpy(tableName, pStname->z, pStname->n);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createSName(SName* pName, SToken* pTableName, SParseContext* pParseCtx, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";

  int32_t  code = TSDB_CODE_SUCCESS;
  char* p  = strnchr(pTableName->z, TS_PATH_DELIMITER[0], pTableName->n, true);

  if (p != NULL) { // db has been specified in sql string so we ignore current db path
    assert(*p == TS_PATH_DELIMITER[0]);

    int32_t dbLen = p - pTableName->z;
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    dbLen = strdequote(name);

    code = tNameSetDbName(pName, pParseCtx->acctId, name, dbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */strdequote(tbname);

    code = tNameFromString(pName, tbname, T_NAME_TABLE);
    if (code != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  } else {  // get current DB name first, and then set it into path
    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    assert(pTableName->n < TSDB_TABLE_FNAME_LEN);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);
    strdequote(name);

    if (pParseCtx->db == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    code = tNameSetDbName(pName, pParseCtx->acctId, pParseCtx->db, strlen(pParseCtx->db));
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg2);
      return code;
    }

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  return code;
}

static int32_t getTableMeta(SInsertParseContext* pCxt, SToken* pTname) {
  SParseContext* pBasicCtx = pCxt->pComCxt;
  SName name = {0};
  createSName(&name, pTname, pBasicCtx, &pCxt->msg);  
  CHECK_CODE(catalogGetTableMeta(pBasicCtx->pCatalog, pBasicCtx->pTransporter, &pBasicCtx->mgmtEpSet, &name, &pCxt->pTableMeta));
  SVgroupInfo vg;
  CHECK_CODE(catalogGetTableHashVgroup(pBasicCtx->pCatalog, pBasicCtx->pTransporter, &pBasicCtx->mgmtEpSet, &name, &vg));
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

static void buildMsgHeader(STableDataBlocks* src, SVgDataBlocks* blocks) {
    SSubmitReq* submit = (SSubmitReq*)blocks->pData;
    submit->header.vgId    = htonl(blocks->vg.vgId);
    submit->header.contLen = htonl(blocks->size);
    submit->length         = submit->header.contLen;
    submit->numOfBlocks    = htonl(blocks->numOfTables);
    SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
    int32_t numOfBlocks = blocks->numOfTables;
    while (numOfBlocks--) {
      int32_t dataLen = blk->dataLen;
      blk->uid = htobe64(blk->uid);
      blk->suid = htobe64(blk->suid);
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
    SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosHashGetDup(pCxt->pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    dst->numOfTables = src->numOfTables;
    dst->size = src->size;
    TSWAP(dst->pData, src->pData, char*);
    buildMsgHeader(src, dst);
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
  if (k <= pDataBlocks->prevTS) {
    pDataBlocks->ordered = false;
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

static int parseTime(char **end, SToken *pToken, int16_t timePrec, int64_t *time, SMsgBuf* pMsgBuf) {
  int32_t   index = 0;
  SToken    sToken;
  int64_t   interval;
  int64_t   ts = 0;
  char* pTokenEnd = *end;

  if (pToken->type == TK_NOW) {
    ts = taosGetTimestamp(timePrec);
  } else if (pToken->type == TK_NK_INTEGER) {
    bool isSigned = false;
    toInteger(pToken->z, pToken->n, 10, &ts, &isSigned);
  } else { // parse the RFC-3339/ISO-8601 timestamp format string
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp format", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = pToken->n; pToken->z[k] != '\0'; k++) {
    if (pToken->z[k] == ' ' || pToken->z[k] == '\t') continue;
    if (pToken->z[k] == ',') {
      *end = pTokenEnd;
      *time = ts;
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

  if (sToken.type == TK_NK_MINUS || sToken.type == TK_NK_PLUS) {
    index = 0;
    valueToken = tStrGetToken(pTokenEnd, &index, false);
    pTokenEnd += index;

    if (valueToken.n < 2) {
      return buildSyntaxErrMsg(pMsgBuf, "value expected in timestamp", sToken.z);
    }

    char unit = 0;
    if (parseAbsoluteDuration(valueToken.z, valueToken.n, &interval, &unit, timePrec) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (sToken.type == TK_NK_PLUS) {
      ts += interval;
    } else {
      ts = ts - interval;
    }

    *end = pTokenEnd;
  }

  *time = ts;
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t checkAndTrimValue(SToken* pToken, uint32_t type, char* tmpTokenBuf, SMsgBuf* pMsgBuf) {
  if ((pToken->type != TK_NOW && pToken->type != TK_NK_INTEGER && pToken->type != TK_NK_STRING && pToken->type != TK_NK_FLOAT && pToken->type != TK_NK_BOOL &&
       pToken->type != TK_NULL && pToken->type != TK_NK_HEX && pToken->type != TK_NK_OCT && pToken->type != TK_NK_BIN) ||
      (pToken->n == 0) || (pToken->type == TK_NK_RP)) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid data or symbol", pToken->z);
  }

  if (IS_NUMERIC_TYPE(type) && pToken->n == 0) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid numeric data", pToken->z);
  }

  // Remove quotation marks
  if (TK_NK_STRING == pToken->type) {
    if (pToken->n >= TSDB_MAX_BYTES_PER_ROW) {
      return buildSyntaxErrMsg(pMsgBuf, "too long string", pToken->z);
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

static bool isNullStr(SToken *pToken) {
  return (pToken->type == TK_NULL) || ((pToken->type == TK_NK_STRING) && (pToken->n != 0) &&
                                       (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0));
}

static FORCE_INLINE int32_t toDouble(SToken *pToken, double *value, char **endPtr) {
  errno = 0;
  *value = strtold(pToken->z, endPtr);

  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_NK_ILLEGAL;
  }

  return pToken->type;
}

static int32_t parseValueToken(char** end, SToken* pToken, SSchema* pSchema, int16_t timePrec, char* tmpTokenBuf, _row_append_fn_t func, void* param, SMsgBuf* pMsgBuf) {
  int64_t iv;
  char   *endptr = NULL;
  bool    isSigned = false;

  int32_t code = checkAndTrimValue(pToken, pSchema->type, tmpTokenBuf, pMsgBuf);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (isNullStr(pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      int64_t tmpVal = 0;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    return func(pMsgBuf, NULL, 0, param);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if ((pToken->type == TK_NK_BOOL || pToken->type == TK_NK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          return func(pMsgBuf, &TRUE_VALUE, pSchema->bytes, param);
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          return func(pMsgBuf, &FALSE_VALUE, pSchema->bytes, param);
        } else {
          return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_NK_INTEGER) {
        return func(pMsgBuf, ((strtoll(pToken->z, NULL, 10) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else if (pToken->type == TK_NK_FLOAT) {
        return func(pMsgBuf, ((strtod(pToken->z, NULL) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
      }
    }

    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "tinyint data overflow", pToken->z);
      }

      uint8_t tmpVal = (uint8_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UTINYINT:{
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned tinyint data overflow", pToken->z);
      }
      uint8_t tmpVal = (uint8_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "smallint data overflow", pToken->z);
      }
      int16_t tmpVal = (int16_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned smallint data overflow", pToken->z);
      }
      uint16_t tmpVal = (uint16_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "int data overflow", pToken->z);
      }
      int32_t tmpVal = (int32_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned int data overflow", pToken->z);
      }
      uint32_t tmpVal = (uint32_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "bigint data overflow", pToken->z);
      }
      return func(pMsgBuf, &iv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned bigint data overflow", pToken->z);
      }
      uint64_t tmpVal = (uint64_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_FLOAT: {
      double dv;
      if (TK_NK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      float tmpVal = (float)dv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double dv;
      if (TK_NK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }
      return func(pMsgBuf, &dv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_BINARY: {
      // Too long values will raise the invalid sql error message
      if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {
        return buildSyntaxErrMsg(pMsgBuf, "string data overflow", pToken->z);
      }

      return func(pMsgBuf, pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_NCHAR: {
      return func(pMsgBuf, pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t tmpVal;
      if (parseTime(end, pToken, timePrec, &tmpVal, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp", pToken->z);
      }

      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }
  }

  return TSDB_CODE_FAILED;
}

typedef struct SMemParam {
  SRowBuilder* rb;
  SSchema*     schema;
  int32_t      toffset;
  col_id_t     colIdx;
} SMemParam;

static FORCE_INLINE int32_t MemRowAppend(SMsgBuf* pMsgBuf, const void* value, int32_t len, void* param) {
  SMemParam*   pa = (SMemParam*)param;
  SRowBuilder* rb = pa->rb;
  if (TSDB_DATA_TYPE_BINARY == pa->schema->type) {
    const char* rowEnd = tdRowEnd(rb->pBuf);
    STR_WITH_SIZE_TO_VARSTR(rowEnd, value, len);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, true, pa->toffset, pa->colIdx);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t     output = 0;
    const char* rowEnd = tdRowEnd(rb->pBuf);
    if (!taosMbsToUcs4(value, len, (TdUcs4*)varDataVal(rowEnd), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(errno));
      return buildSyntaxErrMsg(pMsgBuf, buf, value);
    }
    varDataSetLen(rowEnd, output);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else {
    if (value == NULL) {  // it is a null data
      tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NULL, value, false, pa->toffset,
                          pa->colIdx);
    } else {
      tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, value, false, pa->toffset,
                          pa->colIdx);
    }
  }
  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_name, ...)
static int32_t parseBoundColumns(SInsertParseContext* pCxt, SParsedDataColInfo* pColList, SSchema* pSchema) {
  col_id_t nCols = pColList->numOfCols;

  pColList->numOfBound = 0; 
  pColList->boundNullLen = 0;
  memset(pColList->boundColumns, 0, sizeof(col_id_t) * nCols);
  for (col_id_t i = 0; i < nCols; ++i) {
    pColList->cols[i].valStat = VAL_STAT_NONE;
  }

  SToken sToken;
  bool    isOrdered = true;
  col_id_t lastColIdx = -1;  // last column found
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);

    if (TK_NK_RP == sToken.type) {
      break;
    }

    col_id_t t = lastColIdx + 1;
    col_id_t index = findCol(&sToken, t, nCols, pSchema);
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
    pColList->boundColumns[pColList->numOfBound] = index + PRIMARYKEY_TIMESTAMP_COL_ID;
    ++pColList->numOfBound;
    switch (pSchema[t].type) {
      case TSDB_DATA_TYPE_BINARY:
        pColList->boundNullLen += (sizeof(VarDataOffsetT) + VARSTR_HEADER_SIZE + CHAR_BYTES);
        break;
      case TSDB_DATA_TYPE_NCHAR:
        pColList->boundNullLen += (sizeof(VarDataOffsetT) + VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE);
        break;
      default:
        pColList->boundNullLen += TYPE_BYTES[pSchema[t].type];
        break;
    }
  }

  pColList->orderStatus = isOrdered ? ORDER_STATUS_ORDERED : ORDER_STATUS_DISORDERED;

  if (!isOrdered) {
    pColList->colIdxInfo = taosMemoryCalloc(pColList->numOfBound, sizeof(SBoundIdxInfo));
    if (NULL == pColList->colIdxInfo) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    SBoundIdxInfo* pColIdx = pColList->colIdxInfo;
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].schemaColIdx = pColList->boundColumns[i];
      pColIdx[i].boundIdx = i;
    }
    qsort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), schemaIdxCompar);
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    qsort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), boundIdxCompar);
  }

  memset(&pColList->boundColumns[pColList->numOfBound], 0,
         sizeof(col_id_t) * (pColList->numOfCols - pColList->numOfBound));

  return TSDB_CODE_SUCCESS;
}

typedef struct SKvParam {
  SKVRowBuilder *builder;
  SSchema       *schema;
  char           buf[TSDB_MAX_TAGS_LEN];
} SKvParam;

static int32_t KvRowAppend(SMsgBuf* pMsgBuf, const void *value, int32_t len, void *param) {
  SKvParam* pa = (SKvParam*) param;

  int8_t  type = pa->schema->type;
  int16_t colId = pa->schema->colId;

  if (TSDB_DATA_TYPE_BINARY == type) {
    STR_WITH_SIZE_TO_VARSTR(pa->buf, value, len);
    tdAddColToKVRow(pa->builder, colId, type, pa->buf);
  } else if (TSDB_DATA_TYPE_NCHAR == type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t output = 0;
    if (!taosMbsToUcs4(value, len, (TdUcs4*)varDataVal(pa->buf), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(errno));
      return buildSyntaxErrMsg(pMsgBuf, buf, value);;
    }

    varDataSetLen(pa->buf, output);
    tdAddColToKVRow(pa->builder, colId, type, pa->buf);
  } else {
    tdAddColToKVRow(pa->builder, colId, type, value);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateTbReq(SInsertParseContext* pCxt, const SName* pName, SKVRow row) {
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pName, dbFName);
  pCxt->createTblReq.type = TD_CHILD_TABLE;
  pCxt->createTblReq.dbFName = strdup(dbFName);
  pCxt->createTblReq.name = strdup(pName->tname);
  pCxt->createTblReq.ctbCfg.suid = pCxt->pTableMeta->suid;
  pCxt->createTblReq.ctbCfg.pTag = row;

  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_value, ...)
static int32_t parseTagsClause(SInsertParseContext* pCxt, SSchema* pSchema, uint8_t precision, const SName* pName) {
  if (tdInitKVRowBuilder(&pCxt->tagsBuilder) < 0) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SKvParam param = {.builder = &pCxt->tagsBuilder};
  SToken sToken;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  for (int i = 0; i < pCxt->tags.numOfBound; ++i) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
    SSchema* pTagSchema = &pSchema[pCxt->tags.boundColumns[i] - 1]; // colId starts with 1
    param.schema = pTagSchema;
    CHECK_CODE(parseValueToken(&pCxt->pSql, &sToken, pTagSchema, precision, tmpTokenBuf, KvRowAppend, &param, &pCxt->msg));
  }

  SKVRow row = tdGetKVRowFromBuilder(&pCxt->tagsBuilder);
  if (NULL == row) {
    return buildInvalidOperationMsg(&pCxt->msg, "tag value expected");
  }
  tdSortKVRowByColIdx(row);

  return buildCreateTbReq(pCxt, pName, row);
}

static int32_t cloneTableMeta(STableMeta* pSrc, STableMeta** pDst) {
  *pDst = taosMemoryMalloc(TABLE_META_SIZE(pSrc));
  if (NULL == *pDst) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  memcpy(*pDst, pSrc, TABLE_META_SIZE(pSrc));
  return TSDB_CODE_SUCCESS;
}

static int32_t storeTableMeta(SHashObj* pHash, const char* pName, int32_t len, STableMeta* pMeta) {
  STableMeta* pBackup = NULL;
  if (TSDB_CODE_SUCCESS != cloneTableMeta(pMeta, &pBackup)) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  pBackup->uid = tGenIdPI64();
  return taosHashPut(pHash, pName, len, &pBackup, POINTER_BYTES);
}

// pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t parseUsingClause(SInsertParseContext* pCxt, SToken* pTbnameToken) {
  SName name;
  createSName(&name, pTbnameToken, pCxt->pComCxt, &pCxt->msg);
  char tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(&name, tbFName);
  int32_t len = strlen(tbFName);
  STableMeta** pMeta = taosHashGet(pCxt->pSubTableHashObj, tbFName, len);
  if (NULL != pMeta) {
    return cloneTableMeta(*pMeta, &pCxt->pTableMeta);
  }

  SToken sToken;
  // pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  CHECK_CODE(getTableMeta(pCxt, &sToken));
  if (TSDB_SUPER_TABLE != pCxt->pTableMeta->tableType) {
    return buildInvalidOperationMsg(&pCxt->msg, "create table only from super table is allowed");
  }
  CHECK_CODE(storeTableMeta(pCxt->pSubTableHashObj, tbFName, len, pCxt->pTableMeta));

  SSchema* pTagsSchema = getTableTagSchema(pCxt->pTableMeta);
  setBoundColumnInfo(&pCxt->tags, pTagsSchema, getNumOfTags(pCxt->pTableMeta));

  // pSql -> [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_LP == sToken.type) {
    CHECK_CODE(parseBoundColumns(pCxt, &pCxt->tags, pTagsSchema));
    NEXT_TOKEN(pCxt->pSql, sToken);
  }

  if (TK_TAGS != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "TAGS is expected", sToken.z);
  }
  // pSql -> (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_LP != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, "( is expected", sToken.z);
  }
  CHECK_CODE(parseTagsClause(pCxt, pCxt->pTableMeta->schema, getTableInfo(pCxt->pTableMeta).precision, &name));
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_RP != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, ") is expected", sToken.z);
  }

  return TSDB_CODE_SUCCESS;
}

static int parseOneRow(SInsertParseContext* pCxt, STableDataBlocks* pDataBlocks, int16_t timePrec, int32_t* len, char* tmpTokenBuf) {
  SParsedDataColInfo* spd = &pDataBlocks->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlocks->rowBuilder;
  STSRow*             row = (STSRow*)(pDataBlocks->pData + pDataBlocks->size);  // skip the SSubmitBlk header

  tdSRowResetBuf(pBuilder, row);

  bool isParseBindParam = false;
  SSchema* schema = getTableColumnSchema(pDataBlocks->pTableMeta);
  SMemParam param = {.rb = pBuilder};
  SToken sToken = {0};
  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
    SSchema* pSchema = &schema[spd->boundColumns[i] - 1];
    param.schema = pSchema;
    getSTSRowAppendInfo(schema, pBuilder->rowType, spd, i, &param.toffset, &param.colIdx);
    CHECK_CODE(parseValueToken(&pCxt->pSql, &sToken, pSchema, timePrec, tmpTokenBuf, MemRowAppend, &param, &pCxt->msg));

    if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      TSKEY tsKey = TD_ROW_KEY(row);
      if (checkTimestamp(pDataBlocks, (const char *)&tsKey) != TSDB_CODE_SUCCESS) {
        buildSyntaxErrMsg(&pCxt->msg, "client time/server time can not be mixed up", sToken.z);
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
    }
  }

  if (!isParseBindParam) {
    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      for (int32_t i = 0; i < spd->numOfCols; ++i) {
        if (spd->cols[i].valStat == VAL_STAT_NONE) {  // the primary TS key is not VAL_STAT_NONE
          tdAppendColValToTpRow(pBuilder, TD_VTYPE_NONE, getNullValue(schema[i].type), true, schema[i].type, i,
                                spd->cols[i].toffset);
        }
      }
    }
  }

  // *len = pBuilder->extendedRowSize;
  return TSDB_CODE_SUCCESS;
}

// pSql -> (field1_value, ...) [(field1_value2, ...) ...]
static int32_t parseValues(SInsertParseContext* pCxt, STableDataBlocks* pDataBlock, int maxRows, int32_t* numOfRows) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  (*numOfRows) = 0;
  char tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  SToken sToken;
  while (1) {
    int32_t index = 0;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_NK_LP != sToken.type) {
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
    pDataBlock->size += extendedRowSize; //len;

    NEXT_TOKEN(pCxt->pSql, sToken);
    if (TK_NK_RP != sToken.type) {
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
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, dataBuf, numOfRows)) {
    return buildInvalidOperationMsg(&pCxt->msg, "too many rows in sql, total number of rows should be less than 32767");
  }

  dataBuf->numOfTables = 1;
  pCxt->totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static void destroyCreateSubTbReq(SVCreateTbReq* pReq) {
  taosMemoryFreeClear(pReq->dbFName);
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->ctbCfg.pTag);
}

static void destroyInsertParseContextForTable(SInsertParseContext* pCxt) {
  taosMemoryFreeClear(pCxt->pTableMeta);
  destroyBoundColumnInfo(&pCxt->tags);
  tdDestroyKVRowBuilder(&pCxt->tagsBuilder);
  destroyCreateSubTbReq(&pCxt->createTblReq);
}

static void destroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  taosMemoryFreeClear(pDataBlock->pData);
  if (!pDataBlock->cloned) {
    // free the refcount for metermeta
    if (pDataBlock->pTableMeta != NULL) {
      taosMemoryFreeClear(pDataBlock->pTableMeta);
    }

    destroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
  }
  taosMemoryFreeClear(pDataBlock);
}

static void destroyInsertParseContext(SInsertParseContext* pCxt) {
  destroyInsertParseContextForTable(pCxt);
  taosHashCleanup(pCxt->pVgroupsHashObj);

  destroyBlockHashmap(pCxt->pTableBlockHashObj);
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
        sizeof(SSubmitBlk), getTableInfo(pCxt->pTableMeta).rowSize, pCxt->pTableMeta, &dataBuf, NULL, &pCxt->createTblReq));
        
    if (TK_NK_LP == sToken.type) {
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
    if (TK_NK_FILE == sToken.type) {
      // pSql -> csv_file_path
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (0 == sToken.n || (TK_NK_STRING != sToken.type && TK_NK_ID != sToken.type)) {
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
    CHECK_CODE(mergeTableDataBlocks(pCxt->pTableBlockHashObj, pCxt->pOutput->payloadType, &pCxt->pVgDataBlocks));
  }
  return buildOutput(pCxt);
}

// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
int32_t parseInsertSql(SParseContext* pContext, SQuery** pQuery) {
  SInsertParseContext context = {
    .pComCxt = pContext,
    .pSql = (char*) pContext->pSql,
    .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
    .pTableMeta = NULL,
    .pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false),
    .pTableBlockHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false),
    .pSubTableHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, false),
    .totalNum = 0,
    .pOutput = (SVnodeModifOpStmt*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT)
  };

  if (NULL == context.pVgroupsHashObj || NULL == context.pTableBlockHashObj ||
      NULL == context.pSubTableHashObj || NULL == context.pOutput) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  *pQuery = taosMemoryCalloc(1, sizeof(SQuery));
  if (NULL == *pQuery) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pQuery)->execMode = QUERY_EXEC_MODE_SCHEDULE;
  (*pQuery)->haveResultSet = false;
  (*pQuery)->msgType = TDMT_VND_SUBMIT;
  (*pQuery)->pRoot = (SNode*)context.pOutput;
  context.pOutput->payloadType = PAYLOAD_TYPE_KV;

  int32_t code = skipInsertInto(&context);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBody(&context);
  }
  destroyInsertParseContext(&context);
  return code;
}
