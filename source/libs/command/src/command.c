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

#include "command.h"
#include "catalog.h"
#include "tdatablock.h"
#include "tglobal.h"

static int32_t getSchemaBytes(const SSchema* pSchema) {
  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BINARY:
      return (pSchema->bytes - VARSTR_HEADER_SIZE);
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      return (pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    default:
      return pSchema->bytes;
  }
}

static SSDataBlock* buildDescResultDataBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = DESCRIBE_RESULT_COLS;
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_FIELD_LEN;

  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_TYPE_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;

  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_NOTE_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
}

static void setDescResultIntoDataBlock(SSDataBlock* pBlock, int32_t numOfRows, STableMeta* pMeta) {
  blockDataEnsureCapacity(pBlock, numOfRows);
  pBlock->info.rows = numOfRows;

  // field
  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf[DESCRIBE_RESULT_FIELD_LEN] = {0};
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, pMeta->schema[i].name);
    colDataAppend(pCol1, i, buf, false);
  }

  // Type
  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, tDataTypes[pMeta->schema[i].type].name);
    colDataAppend(pCol2, i, buf, false);
  }

  // Length
  SColumnInfoData* pCol3 = taosArrayGet(pBlock->pDataBlock, 2);
  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t bytes = getSchemaBytes(pMeta->schema + i);
    colDataAppend(pCol3, i, (const char*)&bytes, false);
  }

  // Note
  SColumnInfoData* pCol4 = taosArrayGet(pBlock->pDataBlock, 3);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, i >= pMeta->tableInfo.numOfColumns ? "TAG" : "");
    colDataAppend(pCol4, i, buf, false);
  }
}

static int32_t execDescribe(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  SDescribeStmt* pDesc = (SDescribeStmt*)pStmt;
  int32_t        numOfRows = TABLE_TOTAL_COL_NUM(pDesc->pMeta);

  SSDataBlock* pBlock = buildDescResultDataBlock();
  setDescResultIntoDataBlock(pBlock, numOfRows, pDesc->pMeta);

  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htonl(numOfRows);
  (*pRsp)->numOfCols = htonl(DESCRIBE_RESULT_COLS);

  int32_t len = 0;
  blockCompressEncode(pBlock, (*pRsp)->data, &len, DESCRIBE_RESULT_COLS, false);
  ASSERT(len == rspSize - sizeof(SRetrieveTableRsp));

  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
}

static int32_t execResetQueryCache() { return catalogClearCache(); }


static SSDataBlock* buildCreateDBResultDataBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = SHOW_CREATE_DB_RESULT_COLS;
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(pBlock->info.numOfCols, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_CREATE_DB_RESULT_FIELD1_LEN;

  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_CREATE_DB_RESULT_FIELD2_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
}

int64_t getValOfDiffPrecision(int8_t unit, int64_t val) {
  int64_t v = 0;
  switch (unit) {
    case 's':
      v = val / 1000;
      break;
    case 'm':
      v = val / tsTickPerMin[TSDB_TIME_PRECISION_MILLI];
      break;
    case 'h':
      v = val / (tsTickPerMin[TSDB_TIME_PRECISION_MILLI] * 60);
      break;
    case 'd':
      v = val / (tsTickPerMin[TSDB_TIME_PRECISION_MILLI] * 24 * 60);
      break;
    case 'w':
      v = val / (tsTickPerMin[TSDB_TIME_PRECISION_MILLI] * 24 * 60 * 7);
      break;
    default:
      break;
  }

  return v;
}

char *buildRetension(SArray *pRetension) {
  size_t size = taosArrayGetSize(pRetension);
  if (size == 0) {
    return NULL;
  }

  char       *p1 = taosMemoryCalloc(1, 100);
  SRetention *p = taosArrayGet(pRetension, 0);

  int32_t len = 0;

  int64_t v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
  int64_t v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
  len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);

  if (size > 1) {
    len += sprintf(p1 + len, ",");
    p = taosArrayGet(pRetension, 1);

    v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
  }

  if (size > 2) {
    len += sprintf(p1 + len, ",");
    p = taosArrayGet(pRetension, 2);

    v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
  }

  return p1;
}


static void setCreateDBResultIntoDataBlock(SSDataBlock* pBlock, char *dbFName, SDbCfgInfo* pCfg) {
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_DB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, dbFName);
  colDataAppend(pCol1, 0, buf1, false);

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char             buf2[SHOW_CREATE_DB_RESULT_FIELD2_LEN] = {0};
  int32_t          len = 0;
  char *prec = NULL;
  switch (pCfg->precision) {
    case TSDB_TIME_PRECISION_MILLI:
      prec = TSDB_TIME_PRECISION_MILLI_STR;
      break;
    case TSDB_TIME_PRECISION_MICRO:
      prec = TSDB_TIME_PRECISION_MICRO_STR;
      break;
    case TSDB_TIME_PRECISION_NANO:
      prec = TSDB_TIME_PRECISION_NANO_STR;
      break;
    default:
      prec = "none";
      break;
  }

  char *retentions = buildRetension(pCfg->pRetensions);
  
  len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE DATABASE `%s` BUFFER %d CACHELAST %d COMP %d DURATION %dm "
                    "FSYNC %d MAXROWS %d MINROWS %d KEEP %dm,%dm,%dm PAGES %d PAGESIZE %d PRECISION '%s' REPLICA %d "
                    "STRICT %d WAL %d VGROUPS %d SINGLE_STABLE %d",
                    dbFName, pCfg->buffer, pCfg->cacheLastRow, pCfg->compression, pCfg->daysPerFile,
                    pCfg->fsyncPeriod, pCfg->maxRows, pCfg->minRows, pCfg->daysToKeep0, pCfg->daysToKeep1, pCfg->daysToKeep2,
                    pCfg->pages, pCfg->pageSize, prec, pCfg->replications, pCfg->strict, pCfg->walLevel, pCfg->numOfVgroups,
                    1 == pCfg->numOfStables);

  if (retentions) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, " RETENTIONS %s", retentions);
    taosMemoryFree(retentions);
  }

  (varDataLen(buf2)) = len;
  
  colDataAppend(pCol2, 0, buf2, false);
}


static int32_t execShowCreateDatabase(SShowCreateDatabaseStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = buildCreateDBResultDataBlock();
  setCreateDBResultIntoDataBlock(pBlock, pStmt->dbName, pStmt->pCfg);

  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htonl(1);
  (*pRsp)->numOfCols = htonl(SHOW_CREATE_DB_RESULT_COLS);

  int32_t len = 0;
  blockCompressEncode(pBlock, (*pRsp)->data, &len, SHOW_CREATE_DB_RESULT_COLS, false);
  ASSERT(len == rspSize - sizeof(SRetrieveTableRsp));

  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
}

static int32_t execShowCreateTable(SShowCreateTableStmt* pStmt) { return TSDB_CODE_FAILED; }

static int32_t execShowCreateSTable(SShowCreateTableStmt* pStmt) { return TSDB_CODE_FAILED; }

static int32_t execAlterLocal(SAlterLocalStmt* pStmt) { return TSDB_CODE_FAILED; }

int32_t qExecCommand(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_DESCRIBE_STMT:
      return execDescribe(pStmt, pRsp);
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      return execResetQueryCache();
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return execShowCreateDatabase((SShowCreateDatabaseStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
      return execShowCreateTable((SShowCreateTableStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return execShowCreateSTable((SShowCreateTableStmt*)pStmt);
    case QUERY_NODE_ALTER_LOCAL_STMT:
      return execAlterLocal((SAlterLocalStmt*)pStmt);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}
