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
  char          *pSql;          // input
  SMsgBuf        msg;           // input
  STableMeta* pTableMeta;       // each table
  SParsedDataColInfo tags;      // each table
  SKVRowBuilder tagsBuilder;    // each table
  SHashObj* pVgroupsHashObj;    // global
  SHashObj* pTableBlockHashObj; // global
  SArray* pTableDataBlocks;     // global
  SArray* pVgDataBlocks;        // global
  int32_t totalNum;
  SVnodeModifOpStmtInfo* pOutput;
} SInsertParseContext;

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
    int32_t n = sprintf(fullDbName, "%d.", pCxt->pComCxt->acctId);
    strncpy(fullDbName + n, pStname->z, p - pStname->z);
    strncpy(tableName, p + 1, pStname->n - (p - pStname->z) - 1);
  } else {
    snprintf(fullDbName, TSDB_DB_FNAME_LEN, "%d.%s", pCxt->pComCxt->acctId, pCxt->pComCxt->db);
    strncpy(tableName, pStname->z, pStname->n);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getTableMeta(SInsertParseContext* pCxt, SToken* pTname) {
  SName name = {0};
  createSName(&name, pTname, pCxt->pComCxt, &pCxt->msg);

  char tableName[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&name, tableName);
  SParseContext* pBasicCtx = pCxt->pComCxt;
  CHECK_CODE(catalogGetTableMeta(pBasicCtx->pCatalog, pBasicCtx->pTransporter, &pBasicCtx->mgmtEpSet, &name, &pCxt->pTableMeta));
  SVgroupInfo vg;
  CHECK_CODE(catalogGetTableHashVgroup(pBasicCtx->pCatalog, pBasicCtx->pTransporter, &pBasicCtx->mgmtEpSet, &name, &vg));
  CHECK_CODE(taosHashPut(pCxt->pVgroupsHashObj, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg)));
  pCxt->pTableMeta->vgId = vg.vgId; // todo remove
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
    TSWAP(dst->pData, src->pData, char*);
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

static int parseTime(char **end, SToken *pToken, int16_t timePrec, int64_t *time, SMsgBuf* pMsgBuf) {
  int32_t   index = 0;
  SToken    sToken;
  int64_t   interval;
  int64_t   ts = 0;
  char* pTokenEnd = *end;

  if (pToken->type == TK_NOW) {
    ts = taosGetTimestamp(timePrec);
  } else if (pToken->type == TK_INTEGER) {
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

  if (sToken.type == TK_MINUS || sToken.type == TK_PLUS) {
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

    if (sToken.type == TK_PLUS) {
      ts += interval;
    } else {
      ts = ts - interval;
    }

    *end = pTokenEnd;
  }

  *time = ts;
  return TSDB_CODE_SUCCESS;
}

typedef struct SMemParam {
  SRowBuilder* rb;
  SSchema* schema;
  int32_t toffset;
  int32_t      colIdx;
} SMemParam;

static FORCE_INLINE int32_t MemRowAppend(const void* value, int32_t len, void* param) {
  SMemParam*   pa = (SMemParam*)param;
  SRowBuilder* rb = pa->rb;
  if (TSDB_DATA_TYPE_BINARY == pa->schema->type) {
    const char* rowEnd = tdRowEnd(rb->pBuf);
    STR_WITH_SIZE_TO_VARSTR(rowEnd, value, len);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t     output = 0;
    const char* rowEnd = tdRowEnd(rb->pBuf);
    if (!taosMbsToUcs4(value, len, (char*)varDataVal(rowEnd), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }
    varDataSetLen(rowEnd, output);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else {
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, value, true, pa->toffset, pa->colIdx);
  }
  return TSDB_CODE_SUCCESS;
}

// pSql -> tag1_name, ...)
static int32_t parseBoundColumns(SInsertParseContext* pCxt, SParsedDataColInfo* pColList, SSchema* pSchema) {
  int32_t nCols = pColList->numOfCols;

  pColList->numOfBound = 0; 
  pColList->boundNullLen = 0;
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
    pColList->boundedColumns[pColList->numOfBound] = index + PRIMARYKEY_TIMESTAMP_COL_ID;
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
    CHECK_CODE(parseValueToken(&pCxt->pSql, &sToken, pSchema, precision, tmpTokenBuf, KvRowAppend, &param, &pCxt->msg));
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
  SRowBuilder*        pBuilder = &pDataBlocks->rowBuilder;
  STSRow*             row = (STSRow*)(pDataBlocks->pData + pDataBlocks->size);  // skip the SSubmitBlk header

  tdSRowResetBuf(pBuilder, row);

  bool isParseBindParam = false;
  SSchema* schema = getTableColumnSchema(pDataBlocks->pTableMeta);
  SMemParam param = {.rb = pBuilder};
  SToken sToken = {0};
  // 1. set the parsed value from sql string
  for (int i = 0; i < spd->numOfBound; ++i) {
    NEXT_TOKEN(pCxt->pSql, sToken);
    SSchema *pSchema = &schema[spd->boundedColumns[i] - 1];
    param.schema = pSchema;
    getMemRowAppendInfo(schema, pBuilder->rowType, spd, i, &param.toffset, &param.colIdx);
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
    pDataBlock->size += extendedRowSize; //len;

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

static void destroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  tfree(pDataBlock->pData);
  if (!pDataBlock->cloned) {
    // free the refcount for metermeta
    if (pDataBlock->pTableMeta != NULL) {
      tfree(pDataBlock->pTableMeta);
    }

    destroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
  }
  tfree(pDataBlock);
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
int32_t parseInsertSql(SParseContext* pContext, SVnodeModifOpStmtInfo** pInfo) {
  SInsertParseContext context = {
    .pComCxt = pContext,
    .pSql = (char*) pContext->pSql,
    .msg = {.buf = pContext->pMsg, .len = pContext->msgLen},
    .pTableMeta = NULL,
    .pVgroupsHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false),
    .pTableBlockHashObj = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false),
    .totalNum = 0,
    .pOutput = calloc(1, sizeof(SVnodeModifOpStmtInfo))
  };

  if (NULL == context.pVgroupsHashObj || NULL == context.pTableBlockHashObj || NULL == context.pOutput) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  *pInfo = context.pOutput;
  context.pOutput->nodeType = TSDB_SQL_INSERT;
  context.pOutput->payloadType = PAYLOAD_TYPE_KV;

  int32_t code = skipInsertInto(&context);
  if (TSDB_CODE_SUCCESS == code) {
    code = parseInsertBody(&context);
  }
  destroyInsertParseContext(&context);
  terrno = code;
  return code;
}
