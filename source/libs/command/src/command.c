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
#include "systable.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tgrant.h"
#include "taosdef.h"

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
  (*pRsp)->numOfRows = htobe64((int64_t)pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(numOfCols);

  int32_t len = blockEncode(pBlock, (*pRsp)->data, numOfCols);

  return TSDB_CODE_SUCCESS;
}

static int32_t getSchemaBytes(const SSchema* pSchema) {
  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      return (pSchema->bytes - VARSTR_HEADER_SIZE);
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      return (pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    default:
      return pSchema->bytes;
  }
}

static int32_t buildDescResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_FIELD_LEN, 1);
  int32_t         code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_TYPE_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, tDataTypes[TSDB_DATA_TYPE_INT].bytes, 3);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_NOTE_LEN, 4);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    blockDataDestroy(pBlock);
  }
  return code;
}

static int32_t setDescResultIntoDataBlock(bool sysInfoUser, SSDataBlock* pBlock, int32_t numOfRows, STableMeta* pMeta, int8_t biMode) {
  int32_t blockCap = (biMode != 0) ? numOfRows + 1 : numOfRows;
  blockDataEnsureCapacity(pBlock, blockCap);
  pBlock->info.rows = 0;

  // field
  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  // Type
  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  // Length
  SColumnInfoData* pCol3 = taosArrayGet(pBlock->pDataBlock, 2);
  // Note
  SColumnInfoData* pCol4 = taosArrayGet(pBlock->pDataBlock, 3);
  char             buf[DESCRIBE_RESULT_FIELD_LEN] = {0};
  for (int32_t i = 0; i < numOfRows; ++i) {
    if (invisibleColumn(sysInfoUser, pMeta->tableType, pMeta->schema[i].flags)) {
      continue;
    }
    STR_TO_VARSTR(buf, pMeta->schema[i].name);
    colDataSetVal(pCol1, pBlock->info.rows, buf, false);
    STR_TO_VARSTR(buf, tDataTypes[pMeta->schema[i].type].name);
    colDataSetVal(pCol2, pBlock->info.rows, buf, false);
    int32_t bytes = getSchemaBytes(pMeta->schema + i);
    colDataSetVal(pCol3, pBlock->info.rows, (const char*)&bytes, false);
    if (TSDB_VIEW_TABLE != pMeta->tableType) {
      STR_TO_VARSTR(buf, i >= pMeta->tableInfo.numOfColumns ? "TAG" : "");
    } else {
      STR_TO_VARSTR(buf, "VIEW COL");
    }
    colDataSetVal(pCol4, pBlock->info.rows, buf, false);
    ++(pBlock->info.rows);
  }
  if (pMeta->tableType == TSDB_SUPER_TABLE && biMode != 0) {
    STR_TO_VARSTR(buf, "tbname");
    colDataSetVal(pCol1, pBlock->info.rows, buf, false);
    STR_TO_VARSTR(buf, "VARCHAR");
    colDataSetVal(pCol2, pBlock->info.rows, buf, false);
    int32_t bytes = TSDB_TABLE_NAME_LEN - 1;
    colDataSetVal(pCol3, pBlock->info.rows, (const char*)&bytes, false);
    STR_TO_VARSTR(buf, "TAG");
    colDataSetVal(pCol4, pBlock->info.rows, buf, false);
    ++(pBlock->info.rows);
  }
  if (pBlock->info.rows <= 0) {
    qError("no permission to view any columns");
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t execDescribe(bool sysInfoUser, SNode* pStmt, SRetrieveTableRsp** pRsp, int8_t biMode) {
  SDescribeStmt* pDesc = (SDescribeStmt*)pStmt;
  int32_t        numOfRows = TABLE_TOTAL_COL_NUM(pDesc->pMeta);

  SSDataBlock* pBlock = NULL;
  int32_t      code = buildDescResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setDescResultIntoDataBlock(sysInfoUser, pBlock, numOfRows, pDesc->pMeta, biMode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, DESCRIBE_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

static int32_t execResetQueryCache() { return catalogClearCache(); }

static int32_t buildCreateDBResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_COLS, 1);
  int32_t         code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    blockDataDestroy(pBlock);
  }
  return code;
}

static int32_t buildAliveResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  int32_t         code = blockDataAppendColInfo(pBlock, &infoData);

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    blockDataDestroy(pBlock);
  }
  return code;
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

static char* buildRetension(SArray* pRetension) {
  size_t size = taosArrayGetSize(pRetension);
  if (size == 0) {
    return NULL;
  }

  char*   p1 = taosMemoryCalloc(1, 100);
  int32_t len = 0;

  for (int32_t i = 0; i < size; ++i) {
    SRetention* p = TARRAY_GET_ELEM(pRetension, i);
    int64_t     v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    int64_t     v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    if (i == 0) {
      len += sprintf(p1 + len, "-:%" PRId64 "%c", v2, p->keepUnit);
    } else {
      len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
    }

    if (i < size - 1) {
      len += sprintf(p1 + len, ",");
    }
  }

  return p1;
}

static const char* cacheModelStr(int8_t cacheModel) {
  switch (cacheModel) {
    case TSDB_CACHE_MODEL_NONE:
      return TSDB_CACHE_MODEL_NONE_STR;
    case TSDB_CACHE_MODEL_LAST_ROW:
      return TSDB_CACHE_MODEL_LAST_ROW_STR;
    case TSDB_CACHE_MODEL_LAST_VALUE:
      return TSDB_CACHE_MODEL_LAST_VALUE_STR;
    case TSDB_CACHE_MODEL_BOTH:
      return TSDB_CACHE_MODEL_BOTH_STR;
    default:
      break;
  }
  return TSDB_CACHE_MODEL_NONE_STR;
}

static void setCreateDBResultIntoDataBlock(SSDataBlock* pBlock, char* dbName, char* dbFName, SDbCfgInfo* pCfg) {
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_DB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, dbName);
  colDataSetVal(pCol1, 0, buf1, false);

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
  int32_t dbFNameLen = strlen(dbFName);
  int32_t hashPrefix = 0;
  if (pCfg->hashPrefix > 0) {
    hashPrefix = pCfg->hashPrefix - dbFNameLen - 1;
  } else if (pCfg->hashPrefix < 0) {
    hashPrefix = pCfg->hashPrefix + dbFNameLen + 1;
  }

  if (IS_SYS_DBNAME(dbName)) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE DATABASE `%s`", dbName);
  } else {
    len += sprintf(
        buf2 + VARSTR_HEADER_SIZE,
        "CREATE DATABASE `%s` BUFFER %d CACHESIZE %d CACHEMODEL '%s' COMP %d DURATION %dm "
        "WAL_FSYNC_PERIOD %d MAXROWS %d MINROWS %d STT_TRIGGER %d KEEP %dm,%dm,%dm PAGES %d PAGESIZE %d PRECISION '%s' REPLICA %d "
        "WAL_LEVEL %d VGROUPS %d SINGLE_STABLE %d TABLE_PREFIX %d TABLE_SUFFIX %d TSDB_PAGESIZE %d "
        "WAL_RETENTION_PERIOD %d WAL_RETENTION_SIZE %" PRId64 " KEEP_TIME_OFFSET %d",
        dbName, pCfg->buffer, pCfg->cacheSize, cacheModelStr(pCfg->cacheLast), pCfg->compression, pCfg->daysPerFile,
        pCfg->walFsyncPeriod, pCfg->maxRows, pCfg->minRows,  pCfg->sstTrigger, pCfg->daysToKeep0, pCfg->daysToKeep1, pCfg->daysToKeep2,
        pCfg->pages, pCfg->pageSize, prec, pCfg->replications, pCfg->walLevel, pCfg->numOfVgroups,
        1 == pCfg->numOfStables, hashPrefix, pCfg->hashSuffix, pCfg->tsdbPageSize, pCfg->walRetentionPeriod, pCfg->walRetentionSize,
        pCfg->keepTimeOffset);

    if (retentions) {
      len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, " RETENTIONS %s", retentions);
    }
  }

  taosMemoryFree(retentions);

  (varDataLen(buf2)) = len;

  colDataSetVal(pCol2, 0, buf2, false);
}

#define CHECK_LEADER(n) (row[n] && (fields[n].type == TSDB_DATA_TYPE_VARCHAR && strncasecmp(row[n], "leader", varDataLen((char *)row[n] - VARSTR_HEADER_SIZE)) == 0))
// on this row, if have leader return true else return false
bool existLeaderRole(TAOS_ROW row, TAOS_FIELD* fields, int nFields) {
  // vgroup_id | db_name | tables | v1_dnode | v1_status | v2_dnode | v2_status | v3_dnode | v3_status | v4_dnode |
  // v4_status |  cacheload  | tsma |
  if (nFields != 14) {
    return false;
  }

  // check have leader on cloumn v*_status on 4 6 8 10
  if (CHECK_LEADER(4) || CHECK_LEADER(6) || CHECK_LEADER(8) || CHECK_LEADER(10)) {
    return true;
  }

  return false;
}

// get db alive status, return 1 is alive else return 0
int32_t getAliveStatusFromApi(int64_t* pConnId, char* dbName, int32_t* pStatus) {
  char    sql[128 + TSDB_DB_NAME_LEN] = "select * from information_schema.ins_vgroups";
  int32_t code;

  // filter with db name
  if (dbName && dbName[0] != 0) {
    char str[64 + TSDB_DB_NAME_LEN] = "";
    // test db name exist
    sprintf(str, "show create database %s ;", dbName);
    TAOS_RES* dbRes = taos_query(pConnId, str);
    code = taos_errno(dbRes);
    if (code != TSDB_CODE_SUCCESS) {
      taos_free_result(dbRes);
      return code;
    }
    taos_free_result(dbRes);

    sprintf(str, " where db_name='%s' ;", dbName);
    strcat(sql, str);
  }

  TAOS_RES* res = taos_query(pConnId, sql);
  code = taos_errno(res);
  if (code != TSDB_CODE_SUCCESS) {
    taos_free_result(res);
    return code;
  }

  TAOS_ROW    row = NULL;
  TAOS_FIELD* fields = taos_fetch_fields(res);
  int32_t     nFields = taos_num_fields(res);
  int32_t     nAvailble = 0;
  int32_t     nUnAvailble = 0;

  while ((row = taos_fetch_row(res)) != NULL) {
    if (existLeaderRole(row, fields, nFields)) {
      nAvailble++;
    } else {
      nUnAvailble++;
    }
  }
  taos_free_result(res);

  int32_t status = 0;
  if (nAvailble + nUnAvailble == 0 || nUnAvailble == 0) {
    status = SHOW_STATUS_AVAILABLE;
  } else if (nAvailble > 0 && nUnAvailble > 0) {
    status = SHOW_STATUS_HALF_AVAILABLE;
  } else {
    status = SHOW_STATUS_NOT_AVAILABLE;
  }

  if (pStatus) {
    *pStatus = status;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setAliveResultIntoDataBlock(int64_t* pConnId, SSDataBlock* pBlock, char* dbName) {
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  int32_t          status = 0;
  int32_t          code = getAliveStatusFromApi(pConnId, dbName, &status);
  if (code == TSDB_CODE_SUCCESS) {
    colDataSetVal(pCol1, 0, (const char*)&status, false);
  }
  return code;
}

static int32_t execShowAliveStatus(int64_t* pConnId, SShowAliveStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildAliveResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setAliveResultIntoDataBlock(pConnId, pBlock, pStmt->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_ALIVE_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

static int32_t execShowCreateDatabase(SShowCreateDatabaseStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildCreateDBResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    setCreateDBResultIntoDataBlock(pBlock, pStmt->dbName, pStmt->dbFName, pStmt->pCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_CREATE_DB_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

static int32_t buildCreateTbResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD1_LEN, 1);
  int32_t         code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    blockDataDestroy(pBlock);
  }
  return code;
}

static int32_t buildCreateViewResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_VIEW_RESULT_FIELD1_LEN, 1);
  int32_t         code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_VIEW_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    blockDataDestroy(pBlock);
  }
  return code;
}



void appendColumnFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
    SSchema* pSchema = pCfg->pSchemas + i;
    char     type[32];
    sprintf(type, "%s", tDataTypes[pSchema->type].name);
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type || TSDB_DATA_TYPE_VARBINARY == pSchema->type || TSDB_DATA_TYPE_GEOMETRY == pSchema->type) {
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
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type || TSDB_DATA_TYPE_VARBINARY == pSchema->type || TSDB_DATA_TYPE_GEOMETRY == pSchema->type) {
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

  if (NULL == pCfg->pTags || pCfg->numOfTags <= 0) {
    qError("tag missed in table cfg, pointer:%p, numOfTags:%d", pCfg->pTags, pCfg->numOfTags);
    return TSDB_CODE_APP_ERROR;
  }

  if (tTagIsJson(pTag)) {
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
    if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_GEOMETRY) {
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

void appendTableOptions(char* buf, int32_t* len, SDbCfgInfo* pDbCfg, STableCfg* pCfg) {
  if (pCfg->commentLen > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " COMMENT '%s'", pCfg->pComment);
  } else if (0 == pCfg->commentLen) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " COMMENT ''");
  }

  if (NULL != pDbCfg->pRetensions && pCfg->watermark1 > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " WATERMARK %" PRId64 "a", pCfg->watermark1);
    if (pCfg->watermark2 > 0) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", %" PRId64 "a", pCfg->watermark2);
    }
  }

  if (NULL != pDbCfg->pRetensions && pCfg->delay1 > 0) {
    *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " MAX_DELAY %" PRId64 "a", pCfg->delay1);
    if (pCfg->delay2 > 0) {
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ", %" PRId64 "a", pCfg->delay2);
    }
  }

  int32_t funcNum = taosArrayGetSize(pCfg->pFuncs);
  if (NULL != pDbCfg->pRetensions && funcNum > 0) {
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

  if (TSDB_SUPER_TABLE == pCfg->tableType || TSDB_NORMAL_TABLE == pCfg->tableType) {
    int32_t nSma = 0;
    for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
      if (IS_BSMA_ON(pCfg->pSchemas + i)) {
        ++nSma;
      }
    }

    if (nSma < pCfg->numOfColumns && nSma > 0) {
      bool smaOn = false;
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, " SMA(");
      for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
        if (IS_BSMA_ON(pCfg->pSchemas + i)) {
          if (smaOn) {
            *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ",`%s`", (pCfg->pSchemas + i)->name);
          } else {
            smaOn = true;
            *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, "`%s`", (pCfg->pSchemas + i)->name);
          }
        }
      }
      *len += sprintf(buf + VARSTR_HEADER_SIZE + *len, ")");
    }
  }
}

static int32_t setCreateTBResultIntoDataBlock(SSDataBlock* pBlock, SDbCfgInfo* pDbCfg, char* tbName, STableCfg* pCfg) {
  int32_t code = 0;
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_TB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, tbName);
  colDataSetVal(pCol1, 0, buf1, false);

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char*            buf2 = taosMemoryMalloc(SHOW_CREATE_TB_RESULT_FIELD2_LEN);
  if (NULL == buf2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int32_t len = 0;

  if (TSDB_SUPER_TABLE == pCfg->tableType) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE STABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ") TAGS (");
    appendTagFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  } else if (TSDB_CHILD_TABLE == pCfg->tableType) {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE TABLE `%s` USING `%s` (", tbName, pCfg->stbName);
    appendTagNameFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ") TAGS (");
    code = appendTagValues(buf2, &len, pCfg);
    if (code) {
      taosMemoryFree(buf2);
      return code;
    }
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  } else {
    len += sprintf(buf2 + VARSTR_HEADER_SIZE, "CREATE TABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len += sprintf(buf2 + VARSTR_HEADER_SIZE + len, ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  }

  varDataLen(buf2) = (len > 65535) ? 65535 : len;

  colDataSetVal(pCol2, 0, buf2, false);

  taosMemoryFree(buf2);

  return TSDB_CODE_SUCCESS;
}

static int32_t setCreateViewResultIntoDataBlock(SSDataBlock* pBlock, SShowCreateViewStmt* pStmt) {
  int32_t code = 0;
  blockDataEnsureCapacity(pBlock, 1);
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_VIEW_RESULT_FIELD1_LEN + 1] = {0};
  snprintf(varDataVal(buf1), TSDB_VIEW_FNAME_LEN + 4, "`%s`.`%s`", pStmt->dbName, pStmt->viewName);
  varDataSetLen(buf1, strlen(varDataVal(buf1)));
  colDataSetVal(pCol1, 0, buf1, false);

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char*            buf2 = taosMemoryMalloc(SHOW_CREATE_VIEW_RESULT_FIELD2_LEN);
  if (NULL == buf2) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  SViewMeta* pMeta = pStmt->pViewMeta;
  snprintf(varDataVal(buf2), SHOW_CREATE_VIEW_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE, "CREATE VIEW `%s`.`%s` AS %s", pStmt->dbName, pStmt->viewName, pMeta->querySql);
  int32_t len = strlen(varDataVal(buf2));
  varDataLen(buf2) = (len > 65535) ? 65535 : len;
  colDataSetVal(pCol2, 0, buf2, false);

  taosMemoryFree(buf2);

  return TSDB_CODE_SUCCESS;
}


static int32_t execShowCreateTable(SShowCreateTableStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildCreateTbResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setCreateTBResultIntoDataBlock(pBlock, pStmt->pDbCfg, pStmt->tableName, pStmt->pTableCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_CREATE_TB_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

static int32_t execShowCreateSTable(SShowCreateTableStmt* pStmt, SRetrieveTableRsp** pRsp) {
  STableCfg* pCfg = (STableCfg*)pStmt->pTableCfg;
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
  } else if (0 == strcasecmp(cmd, COMMAND_CATALOG_DEBUG)) {
    code = ctgdHandleDbgCommand(value);
  } else if (0 == strcasecmp(cmd, COMMAND_ENABLE_MEM_DEBUG)) {
    code = taosMemoryDbgInit();
    if (code) {
      qError("failed to init memory dbg, error:%s", tstrerror(code));
      return code;
    }
    tsAsyncLog = false;
    qInfo("memory dbg enabled");
  } else if (0 == strcasecmp(cmd, COMMAND_DISABLE_MEM_DEBUG)) {
    code = taosMemoryDbgInitRestore();
    if (code) {
      qError("failed to restore from memory dbg, error:%s", tstrerror(code));
      return code;
    }
    qInfo("memory dbg disabled");
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

  if (cfgCheckRangeForDynUpdate(tsCfg, pStmt->config, pStmt->value, false)) {
    return terrno;
  }

  if (cfgSetItem(tsCfg, pStmt->config, pStmt->value, CFG_STYPE_ALTER_CMD)) {
    return terrno;
  }

  if (taosCfgDynamicOptions(tsCfg, pStmt->config, false)) {
    return terrno;
  }

_return:

  return TSDB_CODE_SUCCESS;
}

static int32_t buildLocalVariablesResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(SHOW_LOCAL_VARIABLES_RESULT_COLS, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD1_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD2_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD3_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  *pOutput = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t setLocalVariablesResultIntoDataBlock(SSDataBlock* pBlock) {
  int32_t numOfCfg = taosArrayGetSize(tsCfg->array);
  int32_t numOfRows = 0;
  blockDataEnsureCapacity(pBlock, numOfCfg);

  for (int32_t i = 0, c = 0; i < numOfCfg; ++i, c = 0) {
    SConfigItem* pItem = taosArrayGet(tsCfg->array, i);
    // GRANT_CFG_SKIP;

    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pItem->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, name, false);

    char    value[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
    int32_t valueLen = 0;
    cfgDumpItemValue(pItem, &value[VARSTR_HEADER_SIZE], TSDB_CONFIG_VALUE_LEN, &valueLen);
    varDataSetLen(value, valueLen);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, value, false);

    char scope[TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE] = {0};
    cfgDumpItemScope(pItem, &scope[VARSTR_HEADER_SIZE], TSDB_CONFIG_SCOPE_LEN, &valueLen);
    varDataSetLen(scope, valueLen);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, scope, false);

    numOfRows++;
  }

  pBlock->info.rows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

static int32_t execShowLocalVariables(SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildLocalVariablesResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setLocalVariablesResultIntoDataBlock(pBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_LOCAL_VARIABLES_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

static int32_t createSelectResultDataBlock(SNodeList* pProjects, SSDataBlock** pOutput) {
  SSDataBlock* pBlock = createDataBlock();
  if (NULL == pBlock) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pProj = NULL;
  FOREACH(pProj, pProjects) {
    SExprNode*      pExpr = (SExprNode*)pProj;
    SColumnInfoData infoData = {0};
    if (TSDB_DATA_TYPE_NULL == pExpr->resType.type) {
      infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
      infoData.info.bytes = 0;
    } else {
      infoData.info.type = pExpr->resType.type;
      infoData.info.bytes = pExpr->resType.bytes;
    }
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
        colDataSetVal(taosArrayGet(pBlock->pDataBlock, index++), 0, NULL, true);
      } else {
        colDataSetVal(taosArrayGet(pBlock->pDataBlock, index++), 0, nodesGetValueFromNode((SValueNode*)pProj), false);
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
  blockDataDestroy(pBlock);
  return code;
}

static int32_t execShowCreateView(SShowCreateViewStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildCreateViewResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setCreateViewResultIntoDataBlock(pBlock, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_CREATE_VIEW_RESULT_COLS, pRsp);
  }
  blockDataDestroy(pBlock);
  return code;
}

int32_t qExecCommand(int64_t* pConnId, bool sysInfoUser, SNode* pStmt, SRetrieveTableRsp** pRsp, int8_t biMode) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_DESCRIBE_STMT:
      return execDescribe(sysInfoUser, pStmt, pRsp, biMode);
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      return execResetQueryCache();
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return execShowCreateDatabase((SShowCreateDatabaseStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
      return execShowCreateTable((SShowCreateTableStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return execShowCreateSTable((SShowCreateTableStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      return execShowCreateView((SShowCreateViewStmt*)pStmt, pRsp);
    case QUERY_NODE_ALTER_LOCAL_STMT:
      return execAlterLocal((SAlterLocalStmt*)pStmt);
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
      return execShowLocalVariables(pRsp);
    case QUERY_NODE_SELECT_STMT:
      return execSelectWithoutFrom((SSelectStmt*)pStmt, pRsp);
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
      return execShowAliveStatus(pConnId, (SShowAliveStmt*)pStmt, pRsp);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}
