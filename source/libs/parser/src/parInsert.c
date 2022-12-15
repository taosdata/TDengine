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
#include "parInsertData.h"
#include "parInt.h"
#include "parToken.h"
#include "parUtil.h"
#include "query.h"
#include "tglobal.h"
#include "ttime.h"
#include "geosWrapper.h"
#include "ttypes.h"

#define NEXT_TOKEN(pSql, sToken)                \
  do {                                          \
    int32_t index = 0;                          \
    sToken = tStrGetToken(pSql, &index, false); \
    pSql += index;                              \
  } while (0)

#define NEXT_TOKEN_WITH_PREV(pSql, sToken)     \
  do {                                         \
    int32_t index = 0;                         \
    sToken = tStrGetToken(pSql, &index, true); \
    pSql += index;                             \
  } while (0)

#define NEXT_TOKEN_KEEP_SQL(pSql, sToken, index) \
  do {                                           \
    sToken = tStrGetToken(pSql, &index, false);  \
  } while (0)

#define NEXT_VALID_TOKEN(pSql, sToken)        \
  do {                                        \
    sToken.n = tGetToken(pSql, &sToken.type); \
    sToken.z = pSql;                          \
    pSql += sToken.n;                         \
  } while (TK_NK_SPACE == sToken.type)

typedef struct SInsertParseBaseContext {
  SParseContext* pComCxt;
  char*          pSql;
  SMsgBuf        msg;
} SInsertParseBaseContext;

typedef struct SInsertParseContext {
  SParseContext*     pComCxt;             // input
  char*              pSql;                // input
  SMsgBuf            msg;                 // input
  STableMeta*        pTableMeta;          // each table
  SParsedDataColInfo tags;                // each table
  SVCreateTbReq      createTblReq;        // each table
  SHashObj*          pVgroupsHashObj;     // global
  SHashObj*          pTableBlockHashObj;  // global
  SHashObj*          pSubTableHashObj;    // global
  SArray*            pVgDataBlocks;       // global
  SHashObj*          pTableNameHashObj;   // global
  SHashObj*          pDbFNameHashObj;     // global
  SGeosContext       geosCxt;
  int32_t            totalNum;
  SVnodeModifOpStmt* pOutput;
  SStmtCallback*     pStmtCb;
  SParseMetaCache*   pMetaCache;
  char               sTableName[TSDB_TABLE_NAME_LEN];
  char               tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW];
  int64_t            memElapsed;
  int64_t            parRowElapsed;
} SInsertParseContext;

typedef struct SInsertParseSyntaxCxt {
  SParseContext*   pComCxt;
  char*            pSql;
  SMsgBuf          msg;
  SParseMetaCache* pMetaCache;
} SInsertParseSyntaxCxt;

typedef int32_t (*_row_append_fn_t)(SMsgBuf* pMsgBuf, const void* value, int32_t len, void* param);

static uint8_t TRUE_VALUE = (uint8_t)TSDB_TRUE;
static uint8_t FALSE_VALUE = (uint8_t)TSDB_FALSE;

typedef struct SKvParam {
  int16_t  pos;
  SArray*  pTagVals;
  SSchema* schema;
  char     buf[TSDB_MAX_TAGS_LEN];
} SKvParam;

typedef struct SMemParam {
  SRowBuilder* rb;
  SSchema*     schema;
  int32_t      toffset;
  col_id_t     colIdx;
} SMemParam;

#define CHECK_CODE(expr)             \
  do {                               \
    int32_t code = expr;             \
    if (TSDB_CODE_SUCCESS != code) { \
      return code;                   \
    }                                \
  } while (0)

static int32_t skipInsertInto(char** pSql, SMsgBuf* pMsg) {
  SToken sToken;
  NEXT_TOKEN(*pSql, sToken);
  if (TK_INSERT != sToken.type && TK_IMPORT != sToken.type) {
    return buildSyntaxErrMsg(pMsg, "keyword INSERT is expected", sToken.z);
  }
  NEXT_TOKEN(*pSql, sToken);
  if (TK_INTO != sToken.type) {
    return buildSyntaxErrMsg(pMsg, "keyword INTO is expected", sToken.z);
  }
  return TSDB_CODE_SUCCESS;
}

static char* tableNameGetPosition(SToken* pToken, char target) {
  bool inEscape = false;
  bool inQuote = false;
  char quotaStr = 0;

  for (uint32_t i = 0; i < pToken->n; ++i) {
    if (*(pToken->z + i) == target && (!inEscape) && (!inQuote)) {
      return pToken->z + i;
    }

    if (*(pToken->z + i) == TS_ESCAPE_CHAR) {
      if (!inQuote) {
        inEscape = !inEscape;
      }
    }

    if (*(pToken->z + i) == '\'' || *(pToken->z + i) == '"') {
      if (!inEscape) {
        if (!inQuote) {
          quotaStr = *(pToken->z + i);
          inQuote = !inQuote;
        } else if (quotaStr == *(pToken->z + i)) {
          inQuote = !inQuote;
        }
      }
    }
  }

  return NULL;
}

static int32_t createSName(SName* pName, SToken* pTableName, int32_t acctId, const char* dbName, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";
  const char* msg4 = "invalid table name";

  int32_t code = TSDB_CODE_SUCCESS;
  char*   p = tableNameGetPosition(pTableName, TS_PATH_DELIMITER[0]);

  if (p != NULL) {  // db has been specified in sql string so we ignore current db path
    assert(*p == TS_PATH_DELIMITER[0]);

    int32_t dbLen = p - pTableName->z;
    if (dbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    int32_t actualDbLen = strdequote(name);

    code = tNameSetDbName(pName, acctId, name, actualDbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    if (tbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg4);
    }

    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */ strdequote(tbname);

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

    if (dbName == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    code = tNameSetDbName(pName, acctId, dbName, strlen(dbName));
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

static int32_t checkAuth(SInsertParseContext* pCxt, char* pDbFname, bool* pPass) {
  SParseContext* pBasicCtx = pCxt->pComCxt;
  if (pBasicCtx->async) {
    return getUserAuthFromCache(pCxt->pMetaCache, pBasicCtx->pUser, pDbFname, AUTH_TYPE_WRITE, pPass);
  }
  SRequestConnInfo conn = {.pTrans = pBasicCtx->pTransporter,
                           .requestId = pBasicCtx->requestId,
                           .requestObjRefId = pBasicCtx->requestRid,
                           .mgmtEps = pBasicCtx->mgmtEpSet};

  return catalogChkAuth(pBasicCtx->pCatalog, &conn, pBasicCtx->pUser, pDbFname, AUTH_TYPE_WRITE, pPass);
}

static int32_t getTableSchema(SInsertParseContext* pCxt, int32_t tbNo, SName* pTbName, bool isStb,
                              STableMeta** pTableMeta) {
  SParseContext* pBasicCtx = pCxt->pComCxt;
  if (pBasicCtx->async) {
    return getTableMetaFromCacheForInsert(pBasicCtx->pTableMetaPos, pCxt->pMetaCache, tbNo, pTableMeta);
  }
  SRequestConnInfo conn = {.pTrans = pBasicCtx->pTransporter,
                           .requestId = pBasicCtx->requestId,
                           .requestObjRefId = pBasicCtx->requestRid,
                           .mgmtEps = pBasicCtx->mgmtEpSet};

  if (isStb) {
    return catalogGetSTableMeta(pBasicCtx->pCatalog, &conn, pTbName, pTableMeta);
  }
  return catalogGetTableMeta(pBasicCtx->pCatalog, &conn, pTbName, pTableMeta);
}

static int32_t getTableVgroup(SInsertParseContext* pCxt, int32_t tbNo, SName* pTbName, SVgroupInfo* pVg) {
  SParseContext* pBasicCtx = pCxt->pComCxt;
  if (pBasicCtx->async) {
    return getTableVgroupFromCacheForInsert(pBasicCtx->pTableVgroupPos, pCxt->pMetaCache, tbNo, pVg);
  }
  SRequestConnInfo conn = {.pTrans = pBasicCtx->pTransporter,
                           .requestId = pBasicCtx->requestId,
                           .requestObjRefId = pBasicCtx->requestRid,
                           .mgmtEps = pBasicCtx->mgmtEpSet};
  return catalogGetTableHashVgroup(pBasicCtx->pCatalog, &conn, pTbName, pVg);
}

static int32_t getTableMetaImpl(SInsertParseContext* pCxt, int32_t tbNo, SName* name, char* dbFname, bool isStb) {
  CHECK_CODE(getTableSchema(pCxt, tbNo, name, isStb, &pCxt->pTableMeta));
  if (!isStb) {
    SVgroupInfo vg;
    CHECK_CODE(getTableVgroup(pCxt, tbNo, name, &vg));
    CHECK_CODE(taosHashPut(pCxt->pVgroupsHashObj, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg)));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getTableMeta(SInsertParseContext* pCxt, int32_t tbNo, SName* name, char* dbFname) {
  return getTableMetaImpl(pCxt, tbNo, name, dbFname, false);
}

static int32_t getSTableMeta(SInsertParseContext* pCxt, int32_t tbNo, SName* name, char* dbFname) {
  return getTableMetaImpl(pCxt, tbNo, name, dbFname, true);
}

static int32_t getDBCfg(SInsertParseContext* pCxt, const char* pDbFName, SDbCfgInfo* pInfo) {
  SParseContext* pBasicCtx = pCxt->pComCxt;
  if (pBasicCtx->async) {
    CHECK_CODE(getDbCfgFromCache(pCxt->pMetaCache, pDbFName, pInfo));
  } else {
    SRequestConnInfo conn = {.pTrans = pBasicCtx->pTransporter,
                             .requestId = pBasicCtx->requestId,
                             .requestObjRefId = pBasicCtx->requestRid,
                             .mgmtEps = pBasicCtx->mgmtEpSet};
    CHECK_CODE(catalogGetDBCfg(pBasicCtx->pCatalog, &conn, pDbFName, pInfo));
  }
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
  submit->header.vgId = htonl(blocks->vg.vgId);
  submit->header.contLen = htonl(blocks->size);
  submit->length = submit->header.contLen;
  submit->numOfBlocks = htonl(blocks->numOfTables);
  SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
  int32_t     numOfBlocks = blocks->numOfTables;
  while (numOfBlocks--) {
    int32_t dataLen = blk->dataLen;
    int32_t schemaLen = blk->schemaLen;
    blk->uid = htobe64(blk->uid);
    blk->suid = htobe64(blk->suid);
    blk->sversion = htonl(blk->sversion);
    blk->dataLen = htonl(blk->dataLen);
    blk->schemaLen = htonl(blk->schemaLen);
    blk->numOfRows = htonl(blk->numOfRows);
    blk = (SSubmitBlk*)(blk->data + schemaLen + dataLen);
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
    SVgDataBlocks*    dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosHashGetDup(pCxt->pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    dst->numOfTables = src->numOfTables;
    dst->size = src->size;
    TSWAP(dst->pData, src->pData);
    buildMsgHeader(src, dst);
    taosArrayPush(pCxt->pOutput->pDataBlocks, &dst);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t checkTimestamp(STableDataBlocks* pDataBlocks, const char* start) {
  // once the data block is disordered, we do NOT keep previous timestamp any more
  if (!pDataBlocks->ordered) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY k = *(TSKEY*)start;
  if (k <= pDataBlocks->prevTS) {
    pDataBlocks->ordered = false;
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

static int parseTime(char** end, SToken* pToken, int16_t timePrec, int64_t* time, SMsgBuf* pMsgBuf) {
  int32_t index = 0;
  SToken  sToken;
  int64_t interval;
  int64_t ts = 0;
  char*   pTokenEnd = *end;

  if (pToken->type == TK_NOW) {
    ts = taosGetTimestamp(timePrec);
  } else if (pToken->type == TK_TODAY) {
    ts = taosGetTimestampToday(timePrec);
  } else if (pToken->type == TK_NK_INTEGER) {
    toInteger(pToken->z, pToken->n, 10, &ts);
  } else {  // parse the RFC-3339/ISO-8601 timestamp format string
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp format", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = pToken->n; pToken->z[k] != '\0'; k++) {
    if (pToken->z[k] == ' ' || pToken->z[k] == '\t') continue;
    if (pToken->z[k] == '(' && pToken->z[k + 1] == ')') {  // for insert NOW()/TODAY()
      *end = pTokenEnd = &pToken->z[k + 2];
      k++;
      continue;
    }
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

// need to call GEOSFree_r(pGeosCxt->handle, output) later
static int parseGeometry(SGeosContext *pGeosCxt, SToken *pToken, unsigned char **output, size_t *size) {
  int32_t code = TSDB_CODE_FAILED;

  //[ToDo] support to parse WKB as well as WKT
  if (pToken->type == TK_NK_STRING) {
    code = prepareGeomFromText(pGeosCxt);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doGeomFromText(pGeosCxt, pToken->z, output, size);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return code;
}

static FORCE_INLINE int32_t checkAndTrimValue(SToken* pToken, char* tmpTokenBuf, SMsgBuf* pMsgBuf) {
  if ((pToken->type != TK_NOW && pToken->type != TK_TODAY && pToken->type != TK_NK_INTEGER &&
       pToken->type != TK_NK_STRING && pToken->type != TK_NK_FLOAT && pToken->type != TK_NK_BOOL &&
       pToken->type != TK_NULL && pToken->type != TK_NK_HEX && pToken->type != TK_NK_OCT &&
       pToken->type != TK_NK_BIN) ||
      (pToken->n == 0) || (pToken->type == TK_NK_RP)) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid data or symbol", pToken->z);
  }

  // Remove quotation marks
  if (TK_NK_STRING == pToken->type) {
    if (pToken->n >= TSDB_MAX_BYTES_PER_ROW) {
      return buildSyntaxErrMsg(pMsgBuf, "too long string", pToken->z);
    }

    int32_t len = trimString(pToken->z, pToken->n, tmpTokenBuf, TSDB_MAX_BYTES_PER_ROW);
    pToken->z = tmpTokenBuf;
    pToken->n = len;
  }

  return TSDB_CODE_SUCCESS;
}

static bool isNullStr(SToken* pToken) {
  return ((pToken->type == TK_NK_STRING) && (strlen(TSDB_DATA_NULL_STR_L) == pToken->n) &&
          (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0));
}

static bool isNullValue(int8_t dataType, SToken* pToken) {
  return TK_NULL == pToken->type || (!IS_STR_DATA_TYPE(dataType) && isNullStr(pToken));
}

static FORCE_INLINE int32_t toDouble(SToken* pToken, double* value, char** endPtr) {
  errno = 0;
  *value = taosStr2Double(pToken->z, endPtr);

  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_NK_ILLEGAL;
  }

  return pToken->type;
}

static int32_t parseValueToken(char** end, SToken* pToken, SSchema* pSchema, int16_t timePrec, char* tmpTokenBuf,
                               _row_append_fn_t func, void* param, SGeosContext *pGeosCxt, SMsgBuf* pMsgBuf) {
  int64_t  iv;
  uint64_t uv;
  char*    endptr = NULL;

  int32_t code = checkAndTrimValue(pToken, tmpTokenBuf, pMsgBuf);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (isNullValue(pSchema->type, pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      return buildSyntaxErrMsg(pMsgBuf, "primary timestamp should not be null", pToken->z);
    }

    return func(pMsgBuf, NULL, 0, param);
  }

  if (IS_NUMERIC_TYPE(pSchema->type) && pToken->n == 0) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid numeric data", pToken->z);
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
        return func(pMsgBuf, ((taosStr2Int64(pToken->z, NULL, 10) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes,
                    param);
      } else if (pToken->type == TK_NK_FLOAT) {
        return func(pMsgBuf, ((taosStr2Double(pToken->z, NULL) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes,
                    param);
      } else {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
      }
    }

    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "tinyint data overflow", pToken->z);
      }

      uint8_t tmpVal = (uint8_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned tinyint data overflow", pToken->z);
      }
      uint8_t tmpVal = (uint8_t)uv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "smallint data overflow", pToken->z);
      }
      int16_t tmpVal = (int16_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned smallint data overflow", pToken->z);
      }
      uint16_t tmpVal = (uint16_t)uv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "int data overflow", pToken->z);
      }
      int32_t tmpVal = (int32_t)iv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned int data overflow", pToken->z);
      }
      uint32_t tmpVal = (uint32_t)uv;
      return func(pMsgBuf, &tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "bigint data overflow", pToken->z);
      }
      return func(pMsgBuf, &iv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned bigint data overflow", pToken->z);
      }
      return func(pMsgBuf, &uv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_FLOAT: {
      double dv;
      if (TK_NK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) ||
          isnan(dv)) {
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
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }

      return func(pMsgBuf, pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_NCHAR: {
      return func(pMsgBuf, pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_JSON: {
      if (pToken->n > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        return buildSyntaxErrMsg(pMsgBuf, "json string too long than 4095", pToken->z);
      }
      return func(pMsgBuf, pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_GEOMETRY: {
      unsigned char *output = NULL;
      size_t size = 0;

      code = parseGeometry(pGeosCxt, pToken, &output, &size);
      if (code != TSDB_CODE_SUCCESS) {
        code = buildSyntaxErrMsg(pMsgBuf, pGeosCxt->errMsg, pToken->z);
      }
      // Too long values will raise the invalid sql error message
      else if (size > pSchema->bytes) {
        code = generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      else {
        code = func(pMsgBuf, output, size, param);
      }

      if (output) {
        GEOSFree_r(pGeosCxt->handle, output);
        output = NULL;
      }

      return code;
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

static FORCE_INLINE int32_t MemRowAppend(SMsgBuf* pMsgBuf, const void* value, int32_t len, void* param) {
  SMemParam*   pa = (SMemParam*)param;
  SRowBuilder* rb = pa->rb;

  if (value == NULL) {  // it is a null data
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NULL, value, false, pa->toffset, pa->colIdx);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_DATA_TYPE_BINARY == pa->schema->type || TSDB_DATA_TYPE_GEOMETRY == pa->schema->type) {
    const char* rowEnd = tdRowEnd(rb->pBuf);
    STR_WITH_SIZE_TO_VARSTR(rowEnd, value, len);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t     output = 0;
    const char* rowEnd = tdRowEnd(rb->pBuf);
    if (!taosMbsToUcs4(value, len, (TdUcs4*)varDataVal(rowEnd), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      if (errno == E2BIG) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pa->schema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(errno));
      return buildSyntaxErrMsg(pMsgBuf, buf, value);
    }
    varDataSetLen(rowEnd, output);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else {
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, value, false, pa->toffset, pa->colIdx);
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

  SToken   sToken;
  bool     isOrdered = true;
  col_id_t lastColIdx = -1;  // last column found
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);

    if (TK_NK_RP == sToken.type) {
      break;
    }

    char tmpTokenBuf[TSDB_COL_NAME_LEN + 2] = {0};  // used for deleting Escape character backstick(`)
    strncpy(tmpTokenBuf, sToken.z, sToken.n);
    sToken.z = tmpTokenBuf;
    sToken.n = strdequote(sToken.z);

    col_id_t t = lastColIdx + 1;
    col_id_t index = findCol(&sToken, t, nCols, pSchema);
    if (index < 0 && t > 0) {
      index = findCol(&sToken, 0, t, pSchema);
      isOrdered = false;
    }
    if (index < 0) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_INVALID_COLUMN, sToken.z);
    }
    if (pColList->cols[index].valStat == VAL_STAT_HAS) {
      return buildSyntaxErrMsg(&pCxt->msg, "duplicated column name", sToken.z);
    }
    lastColIdx = index;
    pColList->cols[index].valStat = VAL_STAT_HAS;
    pColList->boundColumns[pColList->numOfBound] = index;
    ++pColList->numOfBound;
    switch (pSchema[t].type) {
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_GEOMETRY:
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
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), schemaIdxCompar);
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), boundIdxCompar);
  }

  if (pColList->numOfCols > pColList->numOfBound) {
    memset(&pColList->boundColumns[pColList->numOfBound], 0,
           sizeof(col_id_t) * (pColList->numOfCols - pColList->numOfBound));
  }

  return TSDB_CODE_SUCCESS;
}

static void buildCreateTbReq(SVCreateTbReq* pTbReq, const char* tname, STag* pTag, int64_t suid, const char* sname,
                             SArray* tagName, uint8_t tagNum) {
  pTbReq->type = TD_CHILD_TABLE;
  pTbReq->name = strdup(tname);
  pTbReq->ctb.suid = suid;
  pTbReq->ctb.tagNum = tagNum;
  if (sname) pTbReq->ctb.name = strdup(sname);
  pTbReq->ctb.pTag = (uint8_t*)pTag;
  pTbReq->ctb.tagName = taosArrayDup(tagName);
  pTbReq->ttl = TSDB_DEFAULT_TABLE_TTL;
  pTbReq->commentLen = -1;

  return;
}

static int32_t parseTagToken(char** end, SToken* pToken, SSchema* pSchema, int16_t timePrec, STagVal* val,
                             SMsgBuf* pMsgBuf) {
  int64_t  iv;
  uint64_t uv;
  char*    endptr = NULL;

  if (isNullValue(pSchema->type, pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      return buildSyntaxErrMsg(pMsgBuf, "primary timestamp should not be null", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  //  strcpy(val->colName, pSchema->name);
  val->cid = pSchema->colId;
  val->type = pSchema->type;

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if ((pToken->type == TK_NK_BOOL || pToken->type == TK_NK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          *(int8_t*)(&val->i64) = TRUE_VALUE;
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          *(int8_t*)(&val->i64) = FALSE_VALUE;
        } else {
          return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_NK_INTEGER) {
        *(int8_t*)(&val->i64) = ((taosStr2Int64(pToken->z, NULL, 10) == 0) ? FALSE_VALUE : TRUE_VALUE);
      } else if (pToken->type == TK_NK_FLOAT) {
        *(int8_t*)(&val->i64) = ((taosStr2Double(pToken->z, NULL) == 0) ? FALSE_VALUE : TRUE_VALUE);
      } else {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "tinyint data overflow", pToken->z);
      }

      *(int8_t*)(&val->i64) = iv;
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned tinyint data overflow", pToken->z);
      }
      *(uint8_t*)(&val->i64) = uv;
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "smallint data overflow", pToken->z);
      }
      *(int16_t*)(&val->i64) = iv;
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned smallint data overflow", pToken->z);
      }
      *(uint16_t*)(&val->i64) = uv;
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "int data overflow", pToken->z);
      }
      *(int32_t*)(&val->i64) = iv;
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned int data overflow", pToken->z);
      }
      *(uint32_t*)(&val->i64) = uv;
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "bigint data overflow", pToken->z);
      }

      val->i64 = iv;
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toUInteger(pToken->z, pToken->n, 10, &uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT(uv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned bigint data overflow", pToken->z);
      }
      *(uint64_t*)(&val->i64) = uv;
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      double dv;
      if (TK_NK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) ||
          isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      *(float*)(&val->i64) = dv;
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double dv;
      if (TK_NK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }

      *(double*)(&val->i64) = dv;
      break;
    }

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      // Too long values will raise the invalid sql error message
      if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      val->pData = strdup(pToken->z);
      val->nData = pToken->n;
      break;
    }

    case TSDB_DATA_TYPE_NCHAR: {
      int32_t output = 0;
      void*   p = taosMemoryCalloc(1, pSchema->bytes - VARSTR_HEADER_SIZE);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      if (!taosMbsToUcs4(pToken->z, pToken->n, (TdUcs4*)(p), pSchema->bytes - VARSTR_HEADER_SIZE, &output)) {
        if (errno == E2BIG) {
          taosMemoryFree(p);
          return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
        }
        char buf[512] = {0};
        snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(errno));
        taosMemoryFree(p);
        return buildSyntaxErrMsg(pMsgBuf, buf, pToken->z);
      }
      val->pData = p;
      val->nData = output;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP: {
      if (parseTime(end, pToken, timePrec, &iv, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp", pToken->z);
      }

      val->i64 = iv;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_value, ...)
static int32_t parseTagsClause(SInsertParseContext* pCxt, SSchema* pSchema, uint8_t precision, const char* tName) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* pTagVals = taosArrayInit(pCxt->tags.numOfBound, sizeof(STagVal));
  SArray* tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  SToken  sToken;
  bool    isParseBindParam = false;
  bool    isJson = false;
  STag*   pTag = NULL;
  for (int i = 0; i < pCxt->tags.numOfBound; ++i) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);

    if (sToken.type == TK_NK_QUESTION) {
      isParseBindParam = true;
      if (NULL == pCxt->pStmtCb) {
        code = buildSyntaxErrMsg(&pCxt->msg, "? only used in stmt", sToken.z);
        goto end;
      }

      continue;
    }

    if (isParseBindParam) {
      code = buildInvalidOperationMsg(&pCxt->msg, "no mix usage for ? and tag values");
      goto end;
    }

    SSchema* pTagSchema = &pSchema[pCxt->tags.boundColumns[i]];
    char     tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // todo this can be optimize with parse column
    code = checkAndTrimValue(&sToken, tmpTokenBuf, &pCxt->msg);
    if (code != TSDB_CODE_SUCCESS) {
      goto end;
    }

    if (!isNullValue(pTagSchema->type, &sToken)) {
      taosArrayPush(tagName, pTagSchema->name);
    }
    if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
      if (sToken.n > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        code = buildSyntaxErrMsg(&pCxt->msg, "json string too long than 4095", sToken.z);
        goto end;
      }
      if (isNullValue(pTagSchema->type, &sToken)) {
        code = tTagNew(pTagVals, 1, true, &pTag);
      } else {
        code = parseJsontoTagData(sToken.z, pTagVals, &pTag, &pCxt->msg);
      }
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      isJson = true;
    } else {
      STagVal val = {0};
      code = parseTagToken(&pCxt->pSql, &sToken, pTagSchema, precision, &val, &pCxt->msg);
      if (TSDB_CODE_SUCCESS != code) {
        goto end;
      }

      taosArrayPush(pTagVals, &val);
    }
  }

  if (isParseBindParam) {
    code = TSDB_CODE_SUCCESS;
    goto end;
  }

  if (!isJson && (code = tTagNew(pTagVals, 1, false, &pTag)) != TSDB_CODE_SUCCESS) {
    goto end;
  }

  buildCreateTbReq(&pCxt->createTblReq, tName, pTag, pCxt->pTableMeta->suid, pCxt->sTableName, tagName,
                   pCxt->pTableMeta->tableInfo.numOfTags);

end:
  for (int i = 0; i < taosArrayGetSize(pTagVals); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagVals, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagVals);
  taosArrayDestroy(tagName);
  return code;
}

static int32_t storeTableMeta(SInsertParseContext* pCxt, SHashObj* pHash, int32_t tbNo, SName* pTableName,
                              const char* pName, int32_t len, STableMeta* pMeta) {
  SVgroupInfo vg;
  CHECK_CODE(getTableVgroup(pCxt, tbNo, pTableName, &vg));
  CHECK_CODE(taosHashPut(pCxt->pVgroupsHashObj, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg)));

  pMeta->uid = tbNo;
  pMeta->vgId = vg.vgId;
  pMeta->tableType = TSDB_CHILD_TABLE;

  STableMeta* pBackup = NULL;
  if (TSDB_CODE_SUCCESS != cloneTableMeta(pMeta, &pBackup)) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  return taosHashPut(pHash, pName, len, &pBackup, POINTER_BYTES);
}

static int32_t skipParentheses(SInsertParseSyntaxCxt* pCxt) {
  SToken  sToken;
  int32_t expectRightParenthesis = 1;
  while (1) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    if (TK_NK_LP == sToken.type) {
      ++expectRightParenthesis;
    } else if (TK_NK_RP == sToken.type && 0 == --expectRightParenthesis) {
      break;
    }
    if (0 == sToken.n) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", NULL);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t skipBoundColumns(SInsertParseSyntaxCxt* pCxt) { return skipParentheses(pCxt); }

static int32_t ignoreBoundColumns(SInsertParseContext* pCxt) {
  SInsertParseSyntaxCxt cxt = {.pComCxt = pCxt->pComCxt, .pSql = pCxt->pSql, .msg = pCxt->msg, .pMetaCache = NULL};
  int32_t               code = skipBoundColumns(&cxt);
  pCxt->pSql = cxt.pSql;
  return code;
}

static int32_t skipUsingClause(SInsertParseSyntaxCxt* pCxt);

// pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t ignoreAutoCreateTableClause(SInsertParseContext* pCxt) {
  SToken sToken;
  NEXT_TOKEN(pCxt->pSql, sToken);
  SInsertParseSyntaxCxt cxt = {.pComCxt = pCxt->pComCxt, .pSql = pCxt->pSql, .msg = pCxt->msg, .pMetaCache = NULL};
  int32_t               code = skipUsingClause(&cxt);
  pCxt->pSql = cxt.pSql;
  return code;
}

static int32_t parseTableOptions(SInsertParseContext* pCxt) {
  do {
    int32_t index = 0;
    SToken  sToken;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_TTL == sToken.type) {
      pCxt->pSql += index;
      NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
      if (TK_NK_INTEGER != sToken.type) {
        return buildSyntaxErrMsg(&pCxt->msg, "Invalid option ttl", sToken.z);
      }
      pCxt->createTblReq.ttl = taosStr2Int32(sToken.z, NULL, 10);
      if (pCxt->createTblReq.ttl < 0) {
        return buildSyntaxErrMsg(&pCxt->msg, "Invalid option ttl", sToken.z);
      }
    } else if (TK_COMMENT == sToken.type) {
      pCxt->pSql += index;
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (TK_NK_STRING != sToken.type) {
        return buildSyntaxErrMsg(&pCxt->msg, "Invalid option comment", sToken.z);
      }
      if (sToken.n >= TSDB_TB_COMMENT_LEN) {
        return buildSyntaxErrMsg(&pCxt->msg, "comment too long", sToken.z);
      }
      int32_t len = trimString(sToken.z, sToken.n, pCxt->tmpTokenBuf, TSDB_TB_COMMENT_LEN);
      pCxt->createTblReq.comment = strndup(pCxt->tmpTokenBuf, len);
      if (NULL == pCxt->createTblReq.comment) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pCxt->createTblReq.commentLen = len;
    } else {
      break;
    }
  } while (1);
  return TSDB_CODE_SUCCESS;
}

// pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t parseUsingClause(SInsertParseContext* pCxt, int32_t tbNo, SName* name, char* tbFName) {
  int32_t      len = strlen(tbFName);
  STableMeta** pMeta = taosHashGet(pCxt->pSubTableHashObj, tbFName, len);
  if (NULL != pMeta) {
    CHECK_CODE(ignoreAutoCreateTableClause(pCxt));
    return cloneTableMeta(*pMeta, &pCxt->pTableMeta);
  }

  SToken sToken;
  // pSql -> stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)
  NEXT_TOKEN(pCxt->pSql, sToken);

  SName sname;
  createSName(&sname, &sToken, pCxt->pComCxt->acctId, pCxt->pComCxt->db, &pCxt->msg);
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(&sname, dbFName);
  strcpy(pCxt->sTableName, sname.tname);

  CHECK_CODE(getSTableMeta(pCxt, tbNo, &sname, dbFName));
  if (TSDB_SUPER_TABLE != pCxt->pTableMeta->tableType) {
    return buildInvalidOperationMsg(&pCxt->msg, "create table only from super table is allowed");
  }
  CHECK_CODE(storeTableMeta(pCxt, pCxt->pSubTableHashObj, tbNo, name, tbFName, len, pCxt->pTableMeta));

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
  CHECK_CODE(parseTagsClause(pCxt, pTagsSchema, getTableInfo(pCxt->pTableMeta).precision, name->tname));
  NEXT_VALID_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_COMMA == sToken.type) {
    return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  } else if (TK_NK_RP != sToken.type) {
    return buildSyntaxErrMsg(&pCxt->msg, ") is expected", sToken.z);
  }

  return parseTableOptions(pCxt);
}

static int parseOneRow(SInsertParseContext* pCxt, STableDataBlocks* pDataBlocks, int16_t timePrec, bool* gotRow,
                       char* tmpTokenBuf) {
  SParsedDataColInfo* spd = &pDataBlocks->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlocks->rowBuilder;
  STSRow*             row = (STSRow*)(pDataBlocks->pData + pDataBlocks->size);  // skip the SSubmitBlk header

  tdSRowResetBuf(pBuilder, row);

  bool      isParseBindParam = false;
  SSchema*  schema = getTableColumnSchema(pDataBlocks->pTableMeta);
  SMemParam param = {.rb = pBuilder};
  SToken    sToken = {0};
  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
    SSchema* pSchema = &schema[spd->boundColumns[i]];

    if (sToken.type == TK_NK_QUESTION) {
      isParseBindParam = true;
      if (NULL == pCxt->pStmtCb) {
        return buildSyntaxErrMsg(&pCxt->msg, "? only used in stmt", sToken.z);
      }

      continue;
    }

    if (TK_NK_RP == sToken.type) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
    }

    if (isParseBindParam) {
      return buildInvalidOperationMsg(&pCxt->msg, "no mix usage for ? and values");
    }

    param.schema = pSchema;
    getSTSRowAppendInfo(pBuilder->rowType, spd, i, &param.toffset, &param.colIdx);
    CHECK_CODE(parseValueToken(&pCxt->pSql, &sToken, pSchema, timePrec, tmpTokenBuf, MemRowAppend, &param, &pCxt->geosCxt, &pCxt->msg));

    if (i < spd->numOfBound - 1) {
      NEXT_VALID_TOKEN(pCxt->pSql, sToken);
      if (TK_NK_COMMA != sToken.type) {
        return buildSyntaxErrMsg(&pCxt->msg, ", expected", sToken.z);
      }
    }
  }

  TSKEY tsKey = TD_ROW_KEY(row);
  checkTimestamp(pDataBlocks, (const char*)&tsKey);

  if (!isParseBindParam) {
    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }

    tdSRowEnd(pBuilder);

    *gotRow = true;

#ifdef TD_DEBUG_PRINT_ROW
    STSchema* pSTSchema = tdGetSTSChemaFromSSChema(schema, spd->numOfCols, 1);
    tdSRowPrint(row, pSTSchema, __func__);
    taosMemoryFree(pSTSchema);
#endif
  }

  // *len = pBuilder->extendedRowSize;
  return TSDB_CODE_SUCCESS;
}

// pSql -> (field1_value, ...) [(field1_value2, ...) ...]
static int32_t parseValues(SInsertParseContext* pCxt, STableDataBlocks* pDataBlock, int maxRows, int32_t* numOfRows) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t       extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  (*numOfRows) = 0;
  // char   tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
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

    bool gotRow = false;
    CHECK_CODE(parseOneRow(pCxt, pDataBlock, tinfo.precision, &gotRow, pCxt->tmpTokenBuf));
    if (gotRow) {
      pDataBlock->size += extendedRowSize;  // len;
    }

    NEXT_VALID_TOKEN(pCxt->pSql, sToken);
    if (TK_NK_COMMA == sToken.type) {
      return generateSyntaxErrMsg(&pCxt->msg, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
    } else if (TK_NK_RP != sToken.type) {
      return buildSyntaxErrMsg(&pCxt->msg, ") expected", sToken.z);
    }

    if (gotRow) {
      (*numOfRows)++;
    }
  }

  if (0 == (*numOfRows) && (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT))) {
    return buildSyntaxErrMsg(&pCxt->msg, "no any data points", NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t parseValuesClause(SInsertParseContext* pCxt, STableDataBlocks* dataBuf) {
  int32_t maxNumOfRows;
  CHECK_CODE(allocateMemIfNeed(dataBuf, getExtendedRowSize(dataBuf), &maxNumOfRows));

  int32_t numOfRows = 0;
  CHECK_CODE(parseValues(pCxt, dataBuf, maxNumOfRows, &numOfRows));

  SSubmitBlk* pBlocks = (SSubmitBlk*)(dataBuf->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, dataBuf, numOfRows)) {
    return buildInvalidOperationMsg(&pCxt->msg,
                                    "too many rows in sql, total number of rows should be less than INT32_MAX");
  }

  dataBuf->numOfTables = 1;
  pCxt->totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t parseCsvFile(SInsertParseContext* pCxt, TdFilePtr fp, STableDataBlocks* pDataBlock, int maxRows,
                            int32_t* numOfRows) {
  STableComInfo tinfo = getTableInfo(pDataBlock->pTableMeta);
  int32_t       extendedRowSize = getExtendedRowSize(pDataBlock);
  CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  (*numOfRows) = 0;
  char    tmpTokenBuf[TSDB_MAX_BYTES_PER_ROW] = {0};  // used for deleting Escape character: \\, \', \"
  char*   pLine = NULL;
  int64_t readLen = 0;
  while ((readLen = taosGetLineFile(fp, &pLine)) != -1) {
    if (('\r' == pLine[readLen - 1]) || ('\n' == pLine[readLen - 1])) {
      pLine[--readLen] = '\0';
    }

    if (readLen == 0) {
      continue;
    }

    if ((*numOfRows) >= maxRows || pDataBlock->size + extendedRowSize >= pDataBlock->nAllocSize) {
      int32_t tSize;
      CHECK_CODE(allocateMemIfNeed(pDataBlock, extendedRowSize, &tSize));
      ASSERT(tSize >= maxRows);
      maxRows = tSize;
    }

    strtolower(pLine, pLine);
    char* pRawSql = pCxt->pSql;
    pCxt->pSql = pLine;
    bool gotRow = false;
    CHECK_CODE(parseOneRow(pCxt, pDataBlock, tinfo.precision, &gotRow, tmpTokenBuf));
    if (gotRow) {
      pDataBlock->size += extendedRowSize;  // len;
      (*numOfRows)++;
    }
    pCxt->pSql = pRawSql;
  }

  if (0 == (*numOfRows) && (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT))) {
    return buildSyntaxErrMsg(&pCxt->msg, "no any data points", NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t parseDataFromFile(SInsertParseContext* pCxt, SToken filePath, STableDataBlocks* dataBuf) {
  char filePathStr[TSDB_FILENAME_LEN] = {0};
  if (TK_NK_STRING == filePath.type) {
    trimString(filePath.z, filePath.n, filePathStr, sizeof(filePathStr));
  } else {
    strncpy(filePathStr, filePath.z, filePath.n);
  }
  TdFilePtr fp = taosOpenFile(filePathStr, TD_FILE_READ | TD_FILE_STREAM);
  if (NULL == fp) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  int32_t maxNumOfRows;
  CHECK_CODE(allocateMemIfNeed(dataBuf, getExtendedRowSize(dataBuf), &maxNumOfRows));

  int32_t numOfRows = 0;
  CHECK_CODE(parseCsvFile(pCxt, fp, dataBuf, maxNumOfRows, &numOfRows));

  SSubmitBlk* pBlocks = (SSubmitBlk*)(dataBuf->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, dataBuf, numOfRows)) {
    return buildInvalidOperationMsg(&pCxt->msg,
                                    "too many rows in sql, total number of rows should be less than INT32_MAX");
  }

  dataBuf->numOfTables = 1;
  pCxt->totalNum += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static void destroyInsertParseContextForTable(SInsertParseContext* pCxt) {
  taosMemoryFreeClear(pCxt->pTableMeta);
  destroyBoundColumnInfo(&pCxt->tags);
  tdDestroySVCreateTbReq(&pCxt->createTblReq);
}

static void destroySubTableHashElem(void* p) { taosMemoryFree(*(STableMeta**)p); }

static void destroyInsertParseContext(SInsertParseContext* pCxt) {
  destroyInsertParseContextForTable(pCxt);
  taosHashCleanup(pCxt->pVgroupsHashObj);
  taosHashCleanup(pCxt->pSubTableHashObj);
  taosHashCleanup(pCxt->pTableNameHashObj);
  taosHashCleanup(pCxt->pDbFNameHashObj);

  destroyBlockHashmap(pCxt->pTableBlockHashObj);
  destroyBlockArrayList(pCxt->pVgDataBlocks);

  destroyGeosContext(&pCxt->geosCxt);
}

static int32_t parseTableName(SInsertParseContext* pCxt, SToken* pTbnameToken, SName* pName, char* pDbFName,
                              char* pTbFName) {
  int32_t code = createSName(pName, pTbnameToken, pCxt->pComCxt->acctId, pCxt->pComCxt->db, &pCxt->msg);
  if (TSDB_CODE_SUCCESS == code) {
    tNameExtractFullName(pName, pTbFName);
    code = taosHashPut(pCxt->pTableNameHashObj, pTbFName, strlen(pTbFName), pName, sizeof(SName));
  }
  if (TSDB_CODE_SUCCESS == code) {
    tNameGetFullDbName(pName, pDbFName);
    code = taosHashPut(pCxt->pDbFNameHashObj, pDbFName, strlen(pDbFName), pDbFName, TSDB_DB_FNAME_LEN);
  }
  return code;
}

//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
static int32_t parseInsertBody(SInsertParseContext* pCxt) {
  int32_t tbNum = 0;
  SName   name;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  char    dbFName[TSDB_DB_FNAME_LEN];
  bool    autoCreateTbl = false;

  // for each table
  while (1) {
    SToken sToken;
    char*  tbName = NULL;

    // pSql -> tb_name ...
    NEXT_TOKEN(pCxt->pSql, sToken);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      if (sToken.type && pCxt->pSql[0]) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid charactor in SQL", sToken.z);
      }

      if (0 == pCxt->totalNum && (!TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT))) {
        return buildInvalidOperationMsg(&pCxt->msg, "no data in sql");
      }
      break;
    }

    if (TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT) && tbNum > 0) {
      return buildInvalidOperationMsg(&pCxt->msg, "single table allowed in one stmt");
    }

    destroyInsertParseContextForTable(pCxt);

    if (TK_NK_QUESTION == sToken.type) {
      if (pCxt->pStmtCb) {
        CHECK_CODE((*pCxt->pStmtCb->getTbNameFn)(pCxt->pStmtCb->pStmt, &tbName));

        sToken.z = tbName;
        sToken.n = strlen(tbName);
      } else {
        return buildSyntaxErrMsg(&pCxt->msg, "? only used in stmt", sToken.z);
      }
    }

    SToken tbnameToken = sToken;
    NEXT_TOKEN(pCxt->pSql, sToken);

    if (!pCxt->pComCxt->async || TK_USING == sToken.type) {
      CHECK_CODE(parseTableName(pCxt, &tbnameToken, &name, dbFName, tbFName));
    }

    bool existedUsing = false;
    // USING clause
    if (TK_USING == sToken.type) {
      existedUsing = true;
      CHECK_CODE(parseUsingClause(pCxt, tbNum, &name, tbFName));
      NEXT_TOKEN(pCxt->pSql, sToken);
      autoCreateTbl = true;
    }

    char* pBoundColsStart = NULL;
    if (TK_NK_LP == sToken.type) {
      // pSql -> field1_name, ...)
      pBoundColsStart = pCxt->pSql;
      CHECK_CODE(ignoreBoundColumns(pCxt));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_USING == sToken.type) {
      if (pCxt->pComCxt->async) {
        CHECK_CODE(parseTableName(pCxt, &tbnameToken, &name, dbFName, tbFName));
      }
      CHECK_CODE(parseUsingClause(pCxt, tbNum, &name, tbFName));
      NEXT_TOKEN(pCxt->pSql, sToken);
      autoCreateTbl = true;
    } else if (!existedUsing) {
      CHECK_CODE(getTableMeta(pCxt, tbNum, &name, dbFName));
      if (TSDB_SUPER_TABLE == pCxt->pTableMeta->tableType) {
        return buildInvalidOperationMsg(&pCxt->msg, "insert data into super table is not supported");
      }
    }

    STableDataBlocks* dataBuf = NULL;
    if (pCxt->pComCxt->async) {
      CHECK_CODE(getDataBlockFromList(pCxt->pTableBlockHashObj, &pCxt->pTableMeta->uid, sizeof(pCxt->pTableMeta->uid),
                                      TSDB_DEFAULT_PAYLOAD_SIZE, sizeof(SSubmitBlk),
                                      getTableInfo(pCxt->pTableMeta).rowSize, pCxt->pTableMeta, &dataBuf, NULL,
                                      &pCxt->createTblReq));
    } else {
      CHECK_CODE(getDataBlockFromList(pCxt->pTableBlockHashObj, tbFName, strlen(tbFName), TSDB_DEFAULT_PAYLOAD_SIZE,
                                      sizeof(SSubmitBlk), getTableInfo(pCxt->pTableMeta).rowSize, pCxt->pTableMeta,
                                      &dataBuf, NULL, &pCxt->createTblReq));
    }

    if (NULL != pBoundColsStart) {
      char* pCurrPos = pCxt->pSql;
      pCxt->pSql = pBoundColsStart;
      CHECK_CODE(parseBoundColumns(pCxt, &dataBuf->boundColumnInfo, getTableColumnSchema(pCxt->pTableMeta)));
      pCxt->pSql = pCurrPos;
    }

    if (TK_VALUES == sToken.type) {
      // pSql -> (field1_value, ...) [(field1_value2, ...) ...]
      CHECK_CODE(parseValuesClause(pCxt, dataBuf));
      TSDB_QUERY_SET_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_INSERT);

      tbNum++;
      continue;
    }

    // FILE csv_file_path
    if (TK_FILE == sToken.type) {
      // pSql -> csv_file_path
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (0 == sToken.n || (TK_NK_STRING != sToken.type && TK_NK_ID != sToken.type)) {
        return buildSyntaxErrMsg(&pCxt->msg, "file path is required following keyword FILE", sToken.z);
      }
      CHECK_CODE(parseDataFromFile(pCxt, sToken, dataBuf));
      pCxt->pOutput->insertType = TSDB_QUERY_TYPE_FILE_INSERT;

      tbNum++;
      continue;
    }

    return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", sToken.z);
  }

  qDebug("0x%" PRIx64 " insert input rows: %d", pCxt->pComCxt->requestId, pCxt->totalNum);

  if (TSDB_QUERY_HAS_TYPE(pCxt->pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT)) {
    SParsedDataColInfo* tags = taosMemoryMalloc(sizeof(pCxt->tags));
    if (NULL == tags) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    memcpy(tags, &pCxt->tags, sizeof(pCxt->tags));
    (*pCxt->pStmtCb->setInfoFn)(pCxt->pStmtCb->pStmt, pCxt->pTableMeta, tags, tbFName, autoCreateTbl,
                                pCxt->pVgroupsHashObj, pCxt->pTableBlockHashObj, pCxt->sTableName);

    memset(&pCxt->tags, 0, sizeof(pCxt->tags));
    pCxt->pVgroupsHashObj = NULL;
    pCxt->pTableBlockHashObj = NULL;

    return TSDB_CODE_SUCCESS;
  }

  // merge according to vgId
  if (taosHashGetSize(pCxt->pTableBlockHashObj) > 0) {
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
int32_t parseInsertSql(SParseContext* pContext, SQuery** pQuery, SParseMetaCache* pMetaCache) {
  SInsertParseContext context = {
      .pComCxt = pContext,
      .pSql = (char*)pContext->pSql,
      .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
      .pTableMeta = NULL,
      .createTblReq = {0},
      .pSubTableHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK),
      .pTableNameHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK),
      .pDbFNameHashObj = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK),
      .geosCxt = {0},
      .totalNum = 0,
      .pOutput = (SVnodeModifOpStmt*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT),
      .pStmtCb = pContext->pStmtCb,
      .pMetaCache = pMetaCache,
      .memElapsed = 0,
      .parRowElapsed = 0};

  if (pContext->pStmtCb && *pQuery) {
    (*pContext->pStmtCb->getExecInfoFn)(pContext->pStmtCb->pStmt, &context.pVgroupsHashObj,
                                        &context.pTableBlockHashObj);
    if (NULL == context.pVgroupsHashObj) {
      context.pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
    }
    if (NULL == context.pTableBlockHashObj) {
      context.pTableBlockHashObj =
          taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    }
  } else {
    context.pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
    context.pTableBlockHashObj =
        taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  }

  if (NULL == context.pVgroupsHashObj || NULL == context.pTableBlockHashObj || NULL == context.pSubTableHashObj ||
      NULL == context.pTableNameHashObj || NULL == context.pDbFNameHashObj || NULL == context.pOutput) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(context.pSubTableHashObj, destroySubTableHashElem);

  if (pContext->pStmtCb) {
    TSDB_QUERY_SET_TYPE(context.pOutput->insertType, TSDB_QUERY_TYPE_STMT_INSERT);
  }

  if (NULL == *pQuery) {
    *pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
    if (NULL == *pQuery) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    nodesDestroyNode((*pQuery)->pRoot);
  }

  (*pQuery)->execMode = QUERY_EXEC_MODE_SCHEDULE;
  (*pQuery)->haveResultSet = false;
  (*pQuery)->msgType = TDMT_VND_SUBMIT;
  (*pQuery)->pRoot = (SNode*)context.pOutput;

  if (NULL == (*pQuery)->pTableList) {
    (*pQuery)->pTableList = taosArrayInit(taosHashGetSize(context.pTableNameHashObj), sizeof(SName));
    if (NULL == (*pQuery)->pTableList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (NULL == (*pQuery)->pDbList) {
    (*pQuery)->pDbList = taosArrayInit(taosHashGetSize(context.pDbFNameHashObj), TSDB_DB_FNAME_LEN);
    if (NULL == (*pQuery)->pDbList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  context.pOutput->payloadType = PAYLOAD_TYPE_KV;

  int32_t code = skipInsertInto(&context.pSql, &context.msg);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBody(&context);
  }
  if (TSDB_CODE_SUCCESS == code || NEED_CLIENT_HANDLE_ERROR(code)) {
    SName* pTable = taosHashIterate(context.pTableNameHashObj, NULL);
    while (NULL != pTable) {
      taosArrayPush((*pQuery)->pTableList, pTable);
      pTable = taosHashIterate(context.pTableNameHashObj, pTable);
    }

    char* pDb = taosHashIterate(context.pDbFNameHashObj, NULL);
    while (NULL != pDb) {
      taosArrayPush((*pQuery)->pDbList, pDb);
      pDb = taosHashIterate(context.pDbFNameHashObj, pDb);
    }
  }
  if (pContext->pStmtCb) {
    context.pVgroupsHashObj = NULL;
    context.pTableBlockHashObj = NULL;
  }
  destroyInsertParseContext(&context);
  return code;
}

// pSql -> (field1_value, ...) [(field1_value2, ...) ...]
static int32_t skipValuesClause(SInsertParseSyntaxCxt* pCxt) {
  int32_t numOfRows = 0;
  SToken  sToken;
  while (1) {
    int32_t index = 0;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_NK_LP != sToken.type) {
      break;
    }
    pCxt->pSql += index;

    CHECK_CODE(skipParentheses(pCxt));
    ++numOfRows;
  }
  if (0 == numOfRows) {
    return buildSyntaxErrMsg(&pCxt->msg, "no any data points", NULL);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t skipTagsClause(SInsertParseSyntaxCxt* pCxt) { return skipParentheses(pCxt); }

static int32_t skipTableOptions(SInsertParseSyntaxCxt* pCxt) {
  do {
    int32_t index = 0;
    SToken  sToken;
    NEXT_TOKEN_KEEP_SQL(pCxt->pSql, sToken, index);
    if (TK_TTL == sToken.type || TK_COMMENT == sToken.type) {
      pCxt->pSql += index;
      NEXT_TOKEN_WITH_PREV(pCxt->pSql, sToken);
    } else {
      break;
    }
  } while (1);
  return TSDB_CODE_SUCCESS;
}

// pSql -> [(tag1_name, ...)] TAGS (tag1_value, ...)
static int32_t skipUsingClause(SInsertParseSyntaxCxt* pCxt) {
  SToken sToken;
  NEXT_TOKEN(pCxt->pSql, sToken);
  if (TK_NK_LP == sToken.type) {
    CHECK_CODE(skipBoundColumns(pCxt));
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
  CHECK_CODE(skipTagsClause(pCxt));
  CHECK_CODE(skipTableOptions(pCxt));

  return TSDB_CODE_SUCCESS;
}

static int32_t collectTableMetaKey(SInsertParseSyntaxCxt* pCxt, bool isStable, int32_t tableNo, SToken* pTbToken) {
  SName name = {0};
  CHECK_CODE(createSName(&name, pTbToken, pCxt->pComCxt->acctId, pCxt->pComCxt->db, &pCxt->msg));
  CHECK_CODE(reserveTableMetaInCacheForInsert(&name, isStable ? CATALOG_REQ_TYPE_META : CATALOG_REQ_TYPE_BOTH, tableNo,
                                              pCxt->pMetaCache));
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTableName(const char* pTableName, SMsgBuf* pMsgBuf) {
  if (NULL != strchr(pTableName, '.')) {
    return generateSyntaxErrMsgExt(pMsgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, "The table name cannot contain '.'");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectAutoCreateTableMetaKey(SInsertParseSyntaxCxt* pCxt, int32_t tableNo, SToken* pTbToken) {
  SName name = {0};
  CHECK_CODE(createSName(&name, pTbToken, pCxt->pComCxt->acctId, pCxt->pComCxt->db, &pCxt->msg));
  CHECK_CODE(checkTableName(name.tname, &pCxt->msg));
  CHECK_CODE(reserveTableMetaInCacheForInsert(&name, CATALOG_REQ_TYPE_VGROUP, tableNo, pCxt->pMetaCache));
  return TSDB_CODE_SUCCESS;
}

static int32_t parseInsertBodySyntax(SInsertParseSyntaxCxt* pCxt) {
  bool    hasData = false;
  int32_t tableNo = 0;
  // for each table
  while (1) {
    SToken sToken;

    // pSql -> tb_name ...
    NEXT_TOKEN(pCxt->pSql, sToken);

    // no data in the sql string anymore.
    if (sToken.n == 0) {
      if (sToken.type && pCxt->pSql[0]) {
        return buildSyntaxErrMsg(&pCxt->msg, "invalid charactor in SQL", sToken.z);
      }

      if (!hasData) {
        return buildInvalidOperationMsg(&pCxt->msg, "no data in sql");
      }
      break;
    }

    hasData = false;

    SToken tbnameToken = sToken;
    NEXT_TOKEN(pCxt->pSql, sToken);

    bool existedUsing = false;
    // USING clause
    if (TK_USING == sToken.type) {
      existedUsing = true;
      CHECK_CODE(collectAutoCreateTableMetaKey(pCxt, tableNo, &tbnameToken));
      NEXT_TOKEN(pCxt->pSql, sToken);
      CHECK_CODE(collectTableMetaKey(pCxt, true, tableNo, &sToken));
      CHECK_CODE(skipUsingClause(pCxt));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_NK_LP == sToken.type) {
      // pSql -> field1_name, ...)
      CHECK_CODE(skipBoundColumns(pCxt));
      NEXT_TOKEN(pCxt->pSql, sToken);
    }

    if (TK_USING == sToken.type && !existedUsing) {
      existedUsing = true;
      CHECK_CODE(collectAutoCreateTableMetaKey(pCxt, tableNo, &tbnameToken));
      NEXT_TOKEN(pCxt->pSql, sToken);
      CHECK_CODE(collectTableMetaKey(pCxt, true, tableNo, &sToken));
      CHECK_CODE(skipUsingClause(pCxt));
      NEXT_TOKEN(pCxt->pSql, sToken);
    } else if (!existedUsing) {
      CHECK_CODE(collectTableMetaKey(pCxt, false, tableNo, &tbnameToken));
    }

    ++tableNo;

    if (TK_VALUES == sToken.type) {
      // pSql -> (field1_value, ...) [(field1_value2, ...) ...]
      CHECK_CODE(skipValuesClause(pCxt));
      hasData = true;
      continue;
    }

    // FILE csv_file_path
    if (TK_FILE == sToken.type) {
      // pSql -> csv_file_path
      NEXT_TOKEN(pCxt->pSql, sToken);
      if (0 == sToken.n || (TK_NK_STRING != sToken.type && TK_NK_ID != sToken.type)) {
        return buildSyntaxErrMsg(&pCxt->msg, "file path is required following keyword FILE", sToken.z);
      }
      hasData = true;
      continue;
    }

    return buildSyntaxErrMsg(&pCxt->msg, "keyword VALUES or FILE is expected", sToken.z);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseInsertSyntax(SParseContext* pContext, SQuery** pQuery, SParseMetaCache* pMetaCache) {
  SInsertParseSyntaxCxt context = {.pComCxt = pContext,
                                   .pSql = (char*)pContext->pSql,
                                   .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
                                   .pMetaCache = pMetaCache};
  int32_t               code = skipInsertInto(&context.pSql, &context.msg);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBodySyntax(&context);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
    if (NULL == *pQuery) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}

int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char* msgBuf,
                     int32_t msgBufLen) {
  SMsgBuf msg = {.buf = msgBuf, .len = msgBufLen};
  SToken  sToken;
  int32_t code = 0;
  char*   tbName = NULL;

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n == 0) {
    return buildInvalidOperationMsg(&msg, "empty table name");
  }

  code = createSName(pName, &sToken, acctId, dbName, &msg);
  if (code) {
    return code;
  }

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n > 0) {
    return buildInvalidOperationMsg(&msg, "table name format is wrong");
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash) {
  SVnodeModifOpStmt*  modifyNode = (SVnodeModifOpStmt*)pQuery->pRoot;
  int32_t             code = 0;
  SInsertParseContext insertCtx = {
      .pVgroupsHashObj = pVgHash,
      .pTableBlockHashObj = pBlockHash,
      .pOutput = (SVnodeModifOpStmt*)pQuery->pRoot,
  };

  // merge according to vgId
  if (taosHashGetSize(insertCtx.pTableBlockHashObj) > 0) {
    CHECK_CODE(mergeTableDataBlocks(insertCtx.pTableBlockHashObj, modifyNode->payloadType, &insertCtx.pVgDataBlocks));
  }

  CHECK_CODE(buildOutput(&insertCtx));

  destroyBlockArrayList(insertCtx.pVgDataBlocks);
  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  SParsedDataColInfo* tags = (SParsedDataColInfo*)boundTags;
  if (NULL == tags) {
    return TSDB_CODE_QRY_APP_ERROR;
  }

  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  if (!pTagArray) {
    return buildInvalidOperationMsg(&pBuf, "out of memory");
  }

  SArray* tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (!tagName) {
    return buildInvalidOperationMsg(&pBuf, "out of memory");
  }

  int32_t  code = TSDB_CODE_SUCCESS;
  SSchema* pSchema = getTableTagSchema(pDataBlock->pTableMeta);

  bool  isJson = false;
  STag* pTag = NULL;

  for (int c = 0; c < tags->numOfBound; ++c) {
    if (bind[c].is_null && bind[c].is_null[0]) {
      continue;
    }

    SSchema* pTagSchema = &pSchema[tags->boundColumns[c]];
    int32_t  colLen = pTagSchema->bytes;
    if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
      colLen = bind[c].length[0];
    }
    taosArrayPush(tagName, pTagSchema->name);
    if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
      if (colLen > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        code = buildSyntaxErrMsg(&pBuf, "json string too long than 4095", bind[c].buffer);
        goto end;
      }

      isJson = true;
      char* tmp = taosMemoryCalloc(1, colLen + 1);
      memcpy(tmp, bind[c].buffer, colLen);
      code = parseJsontoTagData(tmp, pTagArray, &pTag, &pBuf);
      taosMemoryFree(tmp);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    } else {
      STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
      //      strcpy(val.colName, pTagSchema->name);
      if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
        val.pData = (uint8_t*)bind[c].buffer;
        val.nData = colLen;
      } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t output = 0;
        void*   p = taosMemoryCalloc(1, colLen * TSDB_NCHAR_SIZE);
        if (p == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto end;
        }
        if (!taosMbsToUcs4(bind[c].buffer, colLen, (TdUcs4*)(p), colLen * TSDB_NCHAR_SIZE, &output)) {
          if (errno == E2BIG) {
            taosMemoryFree(p);
            code = generateSyntaxErrMsg(&pBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
            goto end;
          }
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(errno));
          taosMemoryFree(p);
          code = buildSyntaxErrMsg(&pBuf, buf, bind[c].buffer);
          goto end;
        }
        val.pData = p;
        val.nData = output;
      } else {
        memcpy(&val.i64, bind[c].buffer, colLen);
      }
      taosArrayPush(pTagArray, &val);
    }
  }

  if (!isJson && (code = tTagNew(pTagArray, 1, false, &pTag)) != TSDB_CODE_SUCCESS) {
    goto end;
  }

  SVCreateTbReq tbReq = {0};
  buildCreateTbReq(&tbReq, tName, pTag, suid, sTableName, tagName, pDataBlock->pTableMeta->tableInfo.numOfTags);
  code = buildCreateTbMsg(pDataBlock, &tbReq);
  tdDestroySVCreateTbReq(&tbReq);

end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  taosArrayDestroy(tagName);

  return code;
}

int32_t qBindStmtColsValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*            pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  int32_t             extendedRowSize = getExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t             rowNum = bind->num;

  CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  CHECK_CODE(allocateMemForSize(pDataBlock, extendedRowSize * bind->num));

  for (int32_t r = 0; r < bind->num; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size);  // skip the SSubmitBlk header
    tdSRowResetBuf(pBuilder, row);

    for (int c = 0; c < spd->numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[spd->boundColumns[c]];

      if (bind[c].num != rowNum) {
        return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      }

      param.schema = pColSchema;
      getSTSRowAppendInfo(pBuilder->rowType, spd, c, &param.toffset, &param.colIdx);

      if (bind[c].is_null && bind[c].is_null[r]) {
        if (pColSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
          return buildInvalidOperationMsg(&pBuf, "primary timestamp should not be NULL");
        }

        CHECK_CODE(MemRowAppend(&pBuf, NULL, 0, &param));
      } else {
        if (bind[c].buffer_type != pColSchema->type) {
          return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
        }

        int32_t colLen = pColSchema->bytes;
        if (IS_VAR_DATA_TYPE(pColSchema->type)) {
          colLen = bind[c].length[r];
        }

        CHECK_CODE(MemRowAppend(&pBuf, (char*)bind[c].buffer + bind[c].buffer_length * r, colLen, &param));
      }

      if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
        TSKEY tsKey = TD_ROW_KEY(row);
        checkTimestamp(pDataBlock, (const char*)&tsKey);
      }
    }
    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }
    tdSRowEnd(pBuilder);
#ifdef TD_DEBUG_PRINT_ROW
    STSchema* pSTSchema = tdGetSTSChemaFromSSChema(pSchema, spd->numOfCols, 1);
    tdSRowPrint(row, pSTSchema, __func__);
    taosMemoryFree(pSTSchema);
#endif
    pDataBlock->size += extendedRowSize;
  }

  SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, pDataBlock, bind->num)) {
    return buildInvalidOperationMsg(&pBuf, "too many rows in sql, total number of rows should be less than INT32_MAX");
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtSingleColValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, int32_t colIdx,
                                int32_t rowNum) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*            pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  int32_t             extendedRowSize = getExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  bool                rowStart = (0 == colIdx);
  bool                rowEnd = ((colIdx + 1) == spd->numOfBound);

  if (rowStart) {
    CHECK_CODE(initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));
    CHECK_CODE(allocateMemForSize(pDataBlock, extendedRowSize * bind->num));
  }

  for (int32_t r = 0; r < bind->num; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size + extendedRowSize * r);  // skip the SSubmitBlk header
    if (rowStart) {
      tdSRowResetBuf(pBuilder, row);
    } else {
      tdSRowGetBuf(pBuilder, row);
    }

    SSchema* pColSchema = &pSchema[spd->boundColumns[colIdx]];

    if (bind->num != rowNum) {
      return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
    }

    param.schema = pColSchema;
    getSTSRowAppendInfo(pBuilder->rowType, spd, colIdx, &param.toffset, &param.colIdx);

    if (bind->is_null && bind->is_null[r]) {
      if (pColSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        return buildInvalidOperationMsg(&pBuf, "primary timestamp should not be NULL");
      }

      CHECK_CODE(MemRowAppend(&pBuf, NULL, 0, &param));
    } else {
      if (bind->buffer_type != pColSchema->type) {
        return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      }

      int32_t colLen = pColSchema->bytes;
      if (IS_VAR_DATA_TYPE(pColSchema->type)) {
        colLen = bind->length[r];
      }

      CHECK_CODE(MemRowAppend(&pBuf, (char*)bind->buffer + bind->buffer_length * r, colLen, &param));
    }

    if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
      TSKEY tsKey = TD_ROW_KEY(row);
      checkTimestamp(pDataBlock, (const char*)&tsKey);
    }

    // set the null value for the columns that do not assign values
    if (rowEnd && (spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }
    if (rowEnd) {
      tdSRowEnd(pBuilder);
    }
#ifdef TD_DEBUG_PRINT_ROW
    if (rowEnd) {
      STSchema* pSTSchema = tdGetSTSChemaFromSSChema(pSchema, spd->numOfCols, 1);
      tdSRowPrint(row, pSTSchema, __func__);
      taosMemoryFree(pSTSchema);
    }
#endif
  }

  if (rowEnd) {
    pDataBlock->size += extendedRowSize * bind->num;

    SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
    if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, pDataBlock, bind->num)) {
      return buildInvalidOperationMsg(&pBuf,
                                      "too many rows in sql, total number of rows should be less than INT32_MAX");
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t buildBoundFields(SParsedDataColInfo* boundInfo, SSchema* pSchema, int32_t* fieldNum, TAOS_FIELD_E** fields,
                         uint8_t timePrec) {
  if (fields) {
    *fields = taosMemoryCalloc(boundInfo->numOfBound, sizeof(TAOS_FIELD));
    if (NULL == *fields) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SSchema* schema = &pSchema[boundInfo->boundColumns[0]];
    if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
      (*fields)[0].precision = timePrec;
    }

    for (int32_t i = 0; i < boundInfo->numOfBound; ++i) {
      schema = &pSchema[boundInfo->boundColumns[i]];
      strcpy((*fields)[i].name, schema->name);
      (*fields)[i].type = schema->type;
      (*fields)[i].bytes = schema->bytes;
    }
  }

  *fieldNum = boundInfo->numOfBound;

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtTagFields(void* pBlock, void* boundTags, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SParsedDataColInfo* tags = (SParsedDataColInfo*)boundTags;
  if (NULL == tags) {
    return TSDB_CODE_QRY_APP_ERROR;
  }

  if (pDataBlock->pTableMeta->tableType != TSDB_SUPER_TABLE && pDataBlock->pTableMeta->tableType != TSDB_CHILD_TABLE) {
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }

  SSchema* pSchema = getTableTagSchema(pDataBlock->pTableMeta);
  if (tags->numOfBound <= 0) {
    *fieldNum = 0;
    *fields = NULL;

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(tags, pSchema, fieldNum, fields, 0));

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtColFields(void* pBlock, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataBlocks* pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*          pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  if (pDataBlock->boundColumnInfo.numOfBound <= 0) {
    *fieldNum = 0;
    if (fields) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(&pDataBlock->boundColumnInfo, pSchema, fieldNum, fields,
                              pDataBlock->pTableMeta->tableInfo.precision));

  return TSDB_CODE_SUCCESS;
}

// schemaless logic start

typedef struct SmlExecTableHandle {
  SParsedDataColInfo tags;          // each table
  SVCreateTbReq      createTblReq;  // each table
} SmlExecTableHandle;

typedef struct SmlExecHandle {
  SHashObj*          pBlockHash;
  SmlExecTableHandle tableExecHandle;
  SQuery*            pQuery;
} SSmlExecHandle;

static void smlDestroyTableHandle(void* pHandle) {
  SmlExecTableHandle* handle = (SmlExecTableHandle*)pHandle;
  destroyBoundColumnInfo(&handle->tags);
  tdDestroySVCreateTbReq(&handle->createTblReq);
}

static int32_t smlBoundColumnData(SArray* cols, SParsedDataColInfo* pColList, SSchema* pSchema, bool isTag) {
  col_id_t nCols = pColList->numOfCols;

  pColList->numOfBound = 0;
  pColList->boundNullLen = 0;
  memset(pColList->boundColumns, 0, sizeof(col_id_t) * nCols);
  for (col_id_t i = 0; i < nCols; ++i) {
    pColList->cols[i].valStat = VAL_STAT_NONE;
  }

  bool     isOrdered = true;
  col_id_t lastColIdx = -1;  // last column found
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv*  kv = taosArrayGetP(cols, i);
    SToken   sToken = {.n = kv->keyLen, .z = (char*)kv->key};
    col_id_t t = lastColIdx + 1;
    col_id_t index = ((t == 0 && !isTag) ? 0 : findCol(&sToken, t, nCols, pSchema));
    uDebug("SML, index:%d, t:%d, ncols:%d", index, t, nCols);
    if (index < 0 && t > 0) {
      index = findCol(&sToken, 0, t, pSchema);
      isOrdered = false;
    }
    if (index < 0) {
      uError("smlBoundColumnData. index:%d", index);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (pColList->cols[index].valStat == VAL_STAT_HAS) {
      uError("smlBoundColumnData. already set. index:%d", index);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    lastColIdx = index;
    pColList->cols[index].valStat = VAL_STAT_HAS;
    pColList->boundColumns[pColList->numOfBound] = index;
    ++pColList->numOfBound;
    switch (pSchema[t].type) {
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_GEOMETRY:
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
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), schemaIdxCompar);
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), boundIdxCompar);
  }

  if (pColList->numOfCols > pColList->numOfBound) {
    memset(&pColList->boundColumns[pColList->numOfBound], 0,
           sizeof(col_id_t) * (pColList->numOfCols - pColList->numOfBound));
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief No json tag for schemaless
 *
 * @param cols
 * @param tags
 * @param pSchema
 * @param ppTag
 * @param msg
 * @return int32_t
 */
static int32_t smlBuildTagRow(SArray* cols, SParsedDataColInfo* tags, SSchema* pSchema, STag** ppTag, SArray** tagName,
                              SMsgBuf* msg) {
  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  if (!pTagArray) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  *tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (!*tagName) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < tags->numOfBound; ++i) {
    SSchema* pTagSchema = &pSchema[tags->boundColumns[i]];
    SSmlKv*  kv = taosArrayGetP(cols, i);

    taosArrayPush(*tagName, pTagSchema->name);
    STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
    //    strcpy(val.colName, pTagSchema->name);
    if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
      val.pData = (uint8_t*)kv->value;
      val.nData = kv->length;
    } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t output = 0;
      void*   p = taosMemoryCalloc(1, kv->length * TSDB_NCHAR_SIZE);
      if (p == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      if (!taosMbsToUcs4(kv->value, kv->length, (TdUcs4*)(p), kv->length * TSDB_NCHAR_SIZE, &output)) {
        if (errno == E2BIG) {
          taosMemoryFree(p);
          code = generateSyntaxErrMsg(msg, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
          goto end;
        }
        char buf[512] = {0};
        snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(errno));
        taosMemoryFree(p);
        code = buildSyntaxErrMsg(msg, buf, kv->value);
        goto end;
      }
      val.pData = p;
      val.nData = output;
    } else {
      memcpy(&val.i64, &(kv->value), kv->length);
    }
    taosArrayPush(pTagArray, &val);
  }

  code = tTagNew(pTagArray, 1, false, ppTag);
end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFree(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  return code;
}

int32_t smlBindData(void* handle, SArray* tags, SArray* colsSchema, SArray* cols, bool format, STableMeta* pTableMeta,
                    char* tableName, const char* sTableName, int32_t sTableNameLen, char* msgBuf, int16_t msgBufLen) {
  SMsgBuf pBuf = {.buf = msgBuf, .len = msgBufLen};

  SSmlExecHandle* smlHandle = (SSmlExecHandle*)handle;
  smlDestroyTableHandle(&smlHandle->tableExecHandle);  // free for each table
  SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
  setBoundColumnInfo(&smlHandle->tableExecHandle.tags, pTagsSchema, getNumOfTags(pTableMeta));
  int ret = smlBoundColumnData(tags, &smlHandle->tableExecHandle.tags, pTagsSchema, true);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "bound tags error");
    return ret;
  }
  STag*   pTag = NULL;
  SArray* tagName = NULL;
  ret = smlBuildTagRow(tags, &smlHandle->tableExecHandle.tags, pTagsSchema, &pTag, &tagName, &pBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(tagName);
    return ret;
  }

  buildCreateTbReq(&smlHandle->tableExecHandle.createTblReq, tableName, pTag, pTableMeta->suid, NULL, tagName,
                   pTableMeta->tableInfo.numOfTags);
  taosArrayDestroy(tagName);

  smlHandle->tableExecHandle.createTblReq.ctb.name = taosMemoryMalloc(sTableNameLen + 1);
  memcpy(smlHandle->tableExecHandle.createTblReq.ctb.name, sTableName, sTableNameLen);
  smlHandle->tableExecHandle.createTblReq.ctb.name[sTableNameLen] = 0;

  STableDataBlocks* pDataBlock = NULL;
  ret = getDataBlockFromList(smlHandle->pBlockHash, &pTableMeta->uid, sizeof(pTableMeta->uid),
                             TSDB_DEFAULT_PAYLOAD_SIZE, sizeof(SSubmitBlk), getTableInfo(pTableMeta).rowSize,
                             pTableMeta, &pDataBlock, NULL, &smlHandle->tableExecHandle.createTblReq);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "create data block error");
    return ret;
  }

  SSchema* pSchema = getTableColumnSchema(pTableMeta);

  ret = smlBoundColumnData(colsSchema, &pDataBlock->boundColumnInfo, pSchema, false);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "bound cols error");
    return ret;
  }
  int32_t             extendedRowSize = getExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};

  initRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo);

  int32_t rowNum = taosArrayGetSize(cols);
  if (rowNum <= 0) {
    return buildInvalidOperationMsg(&pBuf, "cols size <= 0");
  }
  ret = allocateMemForSize(pDataBlock, extendedRowSize * rowNum);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "allocate memory error");
    return ret;
  }
  for (int32_t r = 0; r < rowNum; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size);  // skip the SSubmitBlk header
    tdSRowResetBuf(pBuilder, row);
    void*  rowData = taosArrayGetP(cols, r);
    size_t rowDataSize = 0;
    if (format) {
      rowDataSize = taosArrayGetSize(rowData);
    }

    // 1. set the parsed value from sql string
    for (int c = 0, j = 0; c < spd->numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[spd->boundColumns[c]];

      param.schema = pColSchema;
      getSTSRowAppendInfo(pBuilder->rowType, spd, c, &param.toffset, &param.colIdx);

      SSmlKv* kv = NULL;
      if (format) {
        if (j < rowDataSize) {
          kv = taosArrayGetP(rowData, j);
          if (rowDataSize != spd->numOfBound && j != 0 &&
              (kv->keyLen != strlen(pColSchema->name) || strncmp(kv->key, pColSchema->name, kv->keyLen) != 0)) {
            kv = NULL;
          } else {
            j++;
          }
        }
      } else {
        void** p = taosHashGet(rowData, pColSchema->name, strlen(pColSchema->name));
        if (p) kv = *p;
      }

      if (kv) {
        int32_t colLen = kv->length;
        if (pColSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
          //          uError("SML:data before:%" PRId64 ", precision:%d", kv->i, pTableMeta->tableInfo.precision);
          kv->i = convertTimePrecision(kv->i, TSDB_TIME_PRECISION_NANO, pTableMeta->tableInfo.precision);
          //          uError("SML:data after:%" PRId64 ", precision:%d", kv->i, pTableMeta->tableInfo.precision);
        }

        if (IS_VAR_DATA_TYPE(kv->type)) {
          MemRowAppend(&pBuf, kv->value, colLen, &param);
        } else {
          MemRowAppend(&pBuf, &(kv->value), colLen, &param);
        }
      } else {
        pBuilder->hasNone = true;
      }

      if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
        TSKEY tsKey = TD_ROW_KEY(row);
        checkTimestamp(pDataBlock, (const char*)&tsKey);
      }
    }

    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }

    tdSRowEnd(pBuilder);
    pDataBlock->size += extendedRowSize;
  }

  SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, pDataBlock, rowNum)) {
    return buildInvalidOperationMsg(&pBuf, "too many rows in sql, total number of rows should be less than INT32_MAX");
  }

  return TSDB_CODE_SUCCESS;
}

void* smlInitHandle(SQuery* pQuery) {
  SSmlExecHandle* handle = taosMemoryCalloc(1, sizeof(SSmlExecHandle));
  if (!handle) return NULL;
  handle->pBlockHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  handle->pQuery = pQuery;

  return handle;
}

void smlDestroyHandle(void* pHandle) {
  if (!pHandle) return;
  SSmlExecHandle* handle = (SSmlExecHandle*)pHandle;
  destroyBlockHashmap(handle->pBlockHash);
  smlDestroyTableHandle(&handle->tableExecHandle);
  taosMemoryFree(handle);
}

int32_t smlBuildOutput(void* handle, SHashObj* pVgHash) {
  SSmlExecHandle* smlHandle = (SSmlExecHandle*)handle;
  return qBuildStmtOutput(smlHandle->pQuery, pVgHash, smlHandle->pBlockHash);
}
// schemaless logic end
