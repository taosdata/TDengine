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
#include "commandInt.h"
#include "scheduler.h"
#include "tdatablock.h"
#include "tglobal.h"

extern SConfig* tsCfg;

static int32_t buildRetrieveTableRsp(SSDataBlock* pBlock, int32_t numOfCols, SRetrieveTableRsp** pRsp) {
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
  (*pRsp)->numOfRows = htonl(pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(numOfCols);

  int32_t len = 0;
  blockEncode(pBlock, (*pRsp)->data, &len, numOfCols, false);
  ASSERT(len == rspSize - sizeof(SRetrieveTableRsp));

  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
}

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
  SSDataBlock* pBlock = createDataBlock();

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_FIELD_LEN, 1);
  blockDataAppendColInfo(pBlock, &infoData);

  infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_TYPE_LEN, 2);
  blockDataAppendColInfo(pBlock, &infoData);

  infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, tDataTypes[TSDB_DATA_TYPE_INT].bytes, 3);
  blockDataAppendColInfo(pBlock, &infoData);

  infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_NOTE_LEN, 4);
  blockDataAppendColInfo(pBlock, &infoData);
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

  return buildRetrieveTableRsp(pBlock, DESCRIBE_RESULT_COLS, pRsp);
}

static int32_t execResetQueryCache() { return catalogClearCache(); }

static SSDataBlock* buildCreateDBResultDataBlock() {
  SSDataBlock*    pBlock = createDataBlock();
  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_COLS, 1);
  blockDataAppendColInfo(pBlock, &infoData);

  infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_FIELD2_LEN, 2);
  blockDataAppendColInfo(pBlock, &infoData);
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

char* buildRetension(SArray* pRetension) {
  size_t size = taosArrayGetSize(pRetension);
  if (size == 0) {
    return NULL;
  }

  char*       p1 = taosMemoryCalloc(1, 100);
  SRetention* p = taosArrayGet(pRetension, 0);

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

static void setCreateDBResultIntoDataBlock(SSDataBlock* pBlock, char* dbFName, SDbCfgInfo* pCfg) {
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_DB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, dbFName);
  colDataAppend(pCol1, 0, buf1, false);

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char             buf2[SHOW_CREATE_DB_RESULT_FIELD2_LEN] = {0};
  int32_t          len = 0;
  char*            prec = NULL;
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

  char* retentions = buildRetension(pCfg->pRetensions);

  len += sprintf(buf2 + VARSTR_HEADER_SIZE,
                 "CREATE DATABASE `%s` BUFFER %d CACHEMODEL %d COMP %d DURATION %dm "
                 "FSYNC %d MAXROWS %d MINROWS %d KEEP %dm,%dm,%dm PAGES %d PAGESIZE %d PRECISION '%s' REPLICA %d "
                 "STRICT %d WAL %d VGROUPS %d SINGLE_STABLE %d",
                 dbFName, pCfg->buffer, pCfg->cacheLast, pCfg->compression, pCfg->daysPerFile, pCfg->fsyncPeriod,
                 pCfg->maxRows, pCfg->minRows, pCfg->daysToKeep0, pCfg->daysToKeep1, pCfg->daysToKeep2, pCfg->pages,
                 pCfg->pageSize, prec, pCfg->replications, pCfg->strict, pCfg->walLevel, pCfg->numOfVgroups,
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
  return buildRetrieveTableRsp(pBlock, SHOW_CREATE_DB_RESULT_COLS, pRsp);
}

static SSDataBlock* buildCreateTbResultDataBlock() {
  SSDataBlock* pBlock = createDataBlock();

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD1_LEN, 1);
  blockDataAppendColInfo(pBlock, &infoData);

  infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD2_LEN, 2);
  blockDataAppendColInfo(pBlock, &infoData);

  return pBlock;
}

void appendColumnFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
    SSchema* pSchema = pCfg->pSchemas + i;
    char     type[32];
    sprintf(type, "%s", tDataTypes[pSchema->type].name);
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type) {
      sprintf(type + strlen(type), "(%d)", (int32_t)(pSchema->bytes - VARSTR_HEADER_SIZE));
    } else if (TSDB_DATA_TYPE_NCHAR == pSchema->type) {
      sprintf(type + strlen(type), "(%d)", (int32_t)((pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }

    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "%s`%s` %s", ((i > 0) ? ", " : ""), pSchema->name, type);
  }
}

void appendTagFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    char     type[32];
    sprintf(type, "%s", tDataTypes[pSchema->type].name);
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type) {
      sprintf(type + strlen(type), "(%d)", (int32_t)(pSchema->bytes - VARSTR_HEADER_SIZE));
    } else if (TSDB_DATA_TYPE_NCHAR == pSchema->type) {
      sprintf(type + strlen(type), "(%d)", (int32_t)((pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }

    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "%s`%s` %s", ((i > 0) ? ", " : ""), pSchema->name, type);
  }
}

void appendTagNameFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "%s`%s`", ((i > 0) ? ", " : ""), pSchema->name);
  }
}

int32_t appendTagValues(char* buf, int32_t* len, STableCfg* pCfg) {
  SArray* pTagVals = NULL;
  STag*   pTag = (STag*)pCfg->pTags;

  if (pCfg->pTags && tTagIsJson(pTag)) {
    char* pJson = parseTagDatatoJson(pTag);
    if (pJson) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "%s", pJson);
      taosMemoryFree(pJson);
    }

    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tTagToValArray((const STag*)pCfg->pTags, &pTagVals);
  if (code) {
    return code;
  }

  int16_t valueNum = taosArrayGetSize(pTagVals);
  int32_t num = 0;
  int32_t j = 0;
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    if (i > 0) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", ");
    }

    if (j >= valueNum) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "NULL");
      continue;
    }

    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
    if (pSchema->colId > pTagVal->cid) {
      qError("tag value and column mismatch, schemaId:%d, valId:%d", pSchema->colId, pTagVal->cid);
      taosArrayDestroy(pTagVals);
      return TSDB_CODE_APP_ERROR;
    } else if (pSchema->colId == pTagVal->cid) {
      char    type = pTagVal->type;
      int32_t tlen = 0;

      if (IS_VAR_DATA_TYPE(type)) {
        dataConverToStr(buf + VARSTR_HEADER_SIZE + *len, type, pTagVal->pData, pTagVal->nData, &tlen);
      } else {
        dataConverToStr(buf + VARSTR_HEADER_SIZE + *len, type, &pTagVal->i64, tDataTypes[type].bytes, &tlen);
      }
      *len += tlen;
      j++;
    } else {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "NULL");
    }

    /*
    if (type == TSDB_DATA_TYPE_BINARY) {
      if (pTagVal->nData > 0) {
        if (num) {
          *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", ");
        }

        memcpy(buf + VARSTR_HEADER_SIZE + *len, pTagVal->pData, pTagVal->nData);
        *len += pTagVal->nData;
      }
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      if (pTagVal->nData > 0) {
        if (num) {
          *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", ");
        }
        int32_t tlen = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, buf + VARSTR_HEADER_SIZE + *len);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = *(double *)(&pTagVal->i64);
      int    len = 0;
      term = indexTermCreate(suid, ADD_VALUE, type, key, nKey, (const char *)&val, len);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      int val = *(int *)(&pTagVal->i64);
      int len = 0;
      term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_INT, key, nKey, (const char *)&val, len);
    }
    */
  }

  taosArrayDestroy(pTagVals);

  return TSDB_CODE_SUCCESS;
}

void appendTableOptions(char* buf, int32_t* len, STableCfg* pCfg) {
  if (pCfg->commentLen > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " COMMENT '%s'", pCfg->pComment);
  } else if (0 == pCfg->commentLen) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " COMMENT ''");
  }

  if (pCfg->watermark1 > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " WATERMARK %" PRId64 "a", pCfg->watermark1);
    if (pCfg->watermark2 > 0) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", %" PRId64 "a", pCfg->watermark2);
    }
  }

  if (pCfg->delay1 > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " MAX_DELAY %" PRId64 "a", pCfg->delay1);
    if (pCfg->delay2 > 0) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", %" PRId64 "a", pCfg->delay2);
    }
  }

  int32_t funcNum = taosArrayGetSize(pCfg->pFuncs);
  if (funcNum > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " ROLLUP(");
    for (int32_t i = 0; i < funcNum; ++i) {
      char* pFunc = taosArrayGet(pCfg->pFuncs, i);
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "%s%s", ((i > 0) ? ", " : ""), pFunc);
    }
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ")");
  }

  if (pCfg->ttl > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " TTL %d", pCfg->ttl);
  }
}

static int32_t setCreateTBResultIntoDataBlock(SSDataBlock* pBlock, char* tbName, STableCfg* pCfg) {
  int32_t code = 0;
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_TB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, tbName);
  colDataAppend(pCol1, 0, buf1, false);

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char             buf2[SHOW_CREATE_TB_RESULT_FIELD2_LEN] = {0};
  int32_t          len = 0;

  if (TSDB_SUPER_TABLE == pCfg->tableType) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE STABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ") TAGS (");
    appendTagFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
    appendTableOptions(buf2, &len, pCfg);
  } else if (TSDB_CHILD_TABLE == pCfg->tableType) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE TABLE `%s` USING `%s` (", tbName, pCfg->stbName);
    appendTagNameFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ") TAGS (");
    code = appendTagValues(buf2, &len, pCfg);
    if (code) {
      return code;
    }
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
    appendTableOptions(buf2, &len, pCfg);
  } else {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE TABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
  }

  varDataLen(buf2) = len;

  colDataAppend(pCol2, 0, buf2, false);

  return TSDB_CODE_SUCCESS;
}

static int32_t execShowCreateTable(SShowCreateTableStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = buildCreateTbResultDataBlock();
  int32_t      code = setCreateTBResultIntoDataBlock(pBlock, pStmt->tableName, pStmt->pCfg);
  if (code) {
    return code;
  }
  return buildRetrieveTableRsp(pBlock, SHOW_CREATE_TB_RESULT_COLS, pRsp);
}

static int32_t execShowCreateSTable(SShowCreateTableStmt* pStmt, SRetrieveTableRsp** pRsp) {
  STableCfg* pCfg = (STableCfg*)pStmt->pCfg;
  if (TSDB_SUPER_TABLE != pCfg->tableType) {
    terrno = TSDB_CODE_TSC_NOT_STABLE_ERROR;
    return terrno;
  }

  return execShowCreateTable(pStmt, pRsp);
}

static int32_t execAlterCmd(char* cmd, char* value, bool* processed) {
  int32_t code = 0;

  if (0 == strcasecmp(cmd, COMMAND_RESET_LOG)) {
    taosResetLog();
    cfgDumpCfg(tsCfg, 0, false);
  } else if (0 == strcasecmp(cmd, COMMAND_SCHEDULE_POLICY)) {
    code = schedulerUpdatePolicy(atoi(value));
  } else if (0 == strcasecmp(cmd, COMMAND_ENABLE_RESCHEDULE)) {
    code = schedulerEnableReSchedule(atoi(value));
  } else {
    goto _return;
  }

  *processed = true;

_return:

  if (code) {
    terrno = code;
  }

  return code;
}

static int32_t execAlterLocal(SAlterLocalStmt* pStmt) {
  bool processed = false;

  if (execAlterCmd(pStmt->config, pStmt->value, &processed)) {
    return terrno;
  }

  if (processed) {
    goto _return;
  }

  if (cfgSetItem(tsCfg, pStmt->config, pStmt->value, CFG_STYPE_ALTER_CMD)) {
    return terrno;
  }

  if (taosSetCfg(tsCfg, pStmt->config)) {
    return terrno;
  }

_return:

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* buildLocalVariablesResultDataBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(SHOW_LOCAL_VARIABLES_RESULT_COLS, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD1_LEN;

  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD2_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
}

int32_t setLocalVariablesResultIntoDataBlock(SSDataBlock* pBlock) {
  int32_t numOfCfg = taosArrayGetSize(tsCfg->array);
  int32_t numOfRows = 0;
  blockDataEnsureCapacity(pBlock, numOfCfg);

  for (int32_t i = 0, c = 0; i < numOfCfg; ++i, c = 0) {
    SConfigItem* pItem = taosArrayGet(tsCfg->array, i);

    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pItem->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataAppend(pColInfo, i, name, false);

    char    value[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
    int32_t valueLen = 0;
    cfgDumpItemValue(pItem, &value[VARSTR_HEADER_SIZE], TSDB_CONFIG_VALUE_LEN, &valueLen);
    varDataSetLen(value, valueLen);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataAppend(pColInfo, i, value, false);

    numOfRows++;
  }

  pBlock->info.rows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

static int32_t execShowLocalVariables(SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = buildLocalVariablesResultDataBlock();
  int32_t      code = setLocalVariablesResultIntoDataBlock(pBlock);
  if (code) {
    return code;
  }
  return buildRetrieveTableRsp(pBlock, SHOW_LOCAL_VARIABLES_RESULT_COLS, pRsp);
}

static int32_t createSelectResultDataBlock(SNodeList* pProjects, SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pProj = NULL;
  FOREACH(pProj, pProjects) {
    SColumnInfoData infoData = {0};
    infoData.info.type = ((SExprNode*)pProj)->resType.type;
    infoData.info.bytes = ((SExprNode*)pProj)->resType.bytes;
    blockDataAppendColInfo(pBlock, &infoData);
  }
  *pOutput = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t buildSelectResultDataBlock(SNodeList* pProjects, SSDataBlock* pBlock) {
  blockDataEnsureCapacity(pBlock, 1);

  int32_t index = 0;
  SNode*  pProj = NULL;
  FOREACH(pProj, pProjects) {
    if (QUERY_NODE_VALUE != nodeType(pProj)) {
      return TSDB_CODE_PAR_INVALID_SELECTED_EXPR;
    } else {
      if (((SValueNode*)pProj)->isNull) {
        colDataAppend(taosArrayGet(pBlock->pDataBlock, index++), 0, NULL, true);
      } else {
        colDataAppend(taosArrayGet(pBlock->pDataBlock, index++), 0, nodesGetValueFromNode((SValueNode*)pProj), false);
      }
    }
  }

  pBlock->info.rows = 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t execSelectWithoutFrom(SSelectStmt* pSelect, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = createSelectResultDataBlock(pSelect->pProjectionList, &pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSelectResultDataBlock(pSelect->pProjectionList, pBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, LIST_LENGTH(pSelect->pProjectionList), pRsp);
  }
  return code;
}

int32_t qExecCommand(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_DESCRIBE_STMT:
      return execDescribe(pStmt, pRsp);
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      return execResetQueryCache();
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return execShowCreateDatabase((SShowCreateDatabaseStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
      return execShowCreateTable((SShowCreateTableStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return execShowCreateSTable((SShowCreateTableStmt*)pStmt, pRsp);
    case QUERY_NODE_ALTER_LOCAL_STMT:
      return execAlterLocal((SAlterLocalStmt*)pStmt);
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
      return execShowLocalVariables(pRsp);
    case QUERY_NODE_SELECT_STMT:
      return execSelectWithoutFrom((SSelectStmt*)pStmt, pRsp);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}
