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
#include "taosdef.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tgrant.h"

#define COL_DATA_SET_VAL_AND_CHECK(pCol, rows, buf, isNull) \
  do {                                                      \
    int _code = colDataSetVal(pCol, rows, buf, isNull);     \
    if (TSDB_CODE_SUCCESS != _code) {                       \
      terrno = _code;                                       \
      return _code;                                         \
    }                                                       \
  } while (0)

extern SConfig* tsCfg;

static int32_t buildRetrieveTableRsp(SSDataBlock* pBlock, int32_t numOfCols, SRetrieveTableRsp** pRsp) {
  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock) + PAYLOAD_PREFIX_LEN;
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    return terrno;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;

  (*pRsp)->numOfRows = htobe64((int64_t)pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(numOfCols);

  int32_t len = blockEncode(pBlock, (*pRsp)->data + PAYLOAD_PREFIX_LEN, numOfCols);
  if(len < 0) {
    taosMemoryFree(*pRsp);
    return terrno;
  }
  SET_PAYLOAD_LEN((*pRsp)->data, len, len);

  int32_t payloadLen = len + PAYLOAD_PREFIX_LEN;
  (*pRsp)->payloadLen = htonl(payloadLen);
  (*pRsp)->compLen = htonl(payloadLen);

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
  QRY_PARAM_CHECK(pOutput);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_FIELD_LEN, 1);
  code = blockDataAppendColInfo(pBlock, &infoData);
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
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_COPRESS_OPTION_LEN, 5);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_COPRESS_OPTION_LEN, 6);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, DESCRIBE_RESULT_COPRESS_OPTION_LEN, 7);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    (void)blockDataDestroy(pBlock);
  }
  return code;
}

static int32_t setDescResultIntoDataBlock(bool sysInfoUser, SSDataBlock* pBlock, int32_t numOfRows, STableMeta* pMeta,
                                          int8_t biMode) {
  int32_t blockCap = (biMode != 0) ? numOfRows + 1 : numOfRows;
  QRY_ERR_RET(blockDataEnsureCapacity(pBlock, blockCap));
  pBlock->info.rows = 0;

  // field
  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  // Type
  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  // Length
  SColumnInfoData* pCol3 = taosArrayGet(pBlock->pDataBlock, 2);
  // Note
  SColumnInfoData* pCol4 = taosArrayGet(pBlock->pDataBlock, 3);
  // encode
  SColumnInfoData* pCol5 = NULL;
  // compress
  SColumnInfoData* pCol6 = NULL;
  // level
  SColumnInfoData* pCol7 = NULL;
  if (useCompress(pMeta->tableType)) {
    pCol5 = taosArrayGet(pBlock->pDataBlock, 4);
    pCol6 = taosArrayGet(pBlock->pDataBlock, 5);
    pCol7 = taosArrayGet(pBlock->pDataBlock, 6);
  }

  int32_t fillTagCol = 0;
  char    buf[DESCRIBE_RESULT_FIELD_LEN] = {0};
  for (int32_t i = 0; i < numOfRows; ++i) {
    if (invisibleColumn(sysInfoUser, pMeta->tableType, pMeta->schema[i].flags)) {
      continue;
    }
    STR_TO_VARSTR(buf, pMeta->schema[i].name);
    COL_DATA_SET_VAL_AND_CHECK(pCol1, pBlock->info.rows, buf, false);

    STR_TO_VARSTR(buf, tDataTypes[pMeta->schema[i].type].name);
    COL_DATA_SET_VAL_AND_CHECK(pCol2, pBlock->info.rows, buf, false);
    int32_t bytes = getSchemaBytes(pMeta->schema + i);
    COL_DATA_SET_VAL_AND_CHECK(pCol3, pBlock->info.rows, (const char*)&bytes, false);
    if (TSDB_VIEW_TABLE != pMeta->tableType) {
      if (i >= pMeta->tableInfo.numOfColumns) {
        STR_TO_VARSTR(buf, "TAG");
        fillTagCol = 1;
      } else if (i == 1 && pMeta->schema[i].flags & COL_IS_KEY) {
        STR_TO_VARSTR(buf, "PRIMARY KEY")
      } else {
        STR_TO_VARSTR(buf, "");
      }
    } else {
      STR_TO_VARSTR(buf, "VIEW COL");
    }
    COL_DATA_SET_VAL_AND_CHECK(pCol4, pBlock->info.rows, buf, false);
    if (useCompress(pMeta->tableType) && pMeta->schemaExt) {
      if (i < pMeta->tableInfo.numOfColumns) {
        STR_TO_VARSTR(buf, columnEncodeStr(COMPRESS_L1_TYPE_U32(pMeta->schemaExt[i].compress)));
        COL_DATA_SET_VAL_AND_CHECK(pCol5, pBlock->info.rows, buf, false);
        STR_TO_VARSTR(buf, columnCompressStr(COMPRESS_L2_TYPE_U32(pMeta->schemaExt[i].compress)));
        COL_DATA_SET_VAL_AND_CHECK(pCol6, pBlock->info.rows, buf, false);
        STR_TO_VARSTR(buf, columnLevelStr(COMPRESS_L2_TYPE_LEVEL_U32(pMeta->schemaExt[i].compress)));
        COL_DATA_SET_VAL_AND_CHECK(pCol7, pBlock->info.rows, buf, false);
      } else {
        STR_TO_VARSTR(buf, fillTagCol == 0 ? "" : "disabled");
        COL_DATA_SET_VAL_AND_CHECK(pCol5, pBlock->info.rows, buf, false);
        STR_TO_VARSTR(buf, fillTagCol == 0 ? "" : "disabled");
        COL_DATA_SET_VAL_AND_CHECK(pCol6, pBlock->info.rows, buf, false);
        STR_TO_VARSTR(buf, fillTagCol == 0 ? "" : "disabled");
        COL_DATA_SET_VAL_AND_CHECK(pCol7, pBlock->info.rows, buf, false);
      }
    }

    fillTagCol = 0;

    ++(pBlock->info.rows);
  }
  if (pMeta->tableType == TSDB_SUPER_TABLE && biMode != 0) {
    STR_TO_VARSTR(buf, "tbname");
    COL_DATA_SET_VAL_AND_CHECK(pCol1, pBlock->info.rows, buf, false);
    STR_TO_VARSTR(buf, "VARCHAR");
    COL_DATA_SET_VAL_AND_CHECK(pCol2, pBlock->info.rows, buf, false);
    int32_t bytes = TSDB_TABLE_NAME_LEN - 1;
    COL_DATA_SET_VAL_AND_CHECK(pCol3, pBlock->info.rows, (const char*)&bytes, false);
    STR_TO_VARSTR(buf, "TAG");
    COL_DATA_SET_VAL_AND_CHECK(pCol4, pBlock->info.rows, buf, false);
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
    if (pDesc->pMeta && useCompress(pDesc->pMeta->tableType) && pDesc->pMeta->schemaExt) {
      code = buildRetrieveTableRsp(pBlock, DESCRIBE_RESULT_COLS_COMPRESS, pRsp);
    } else {
      code = buildRetrieveTableRsp(pBlock, DESCRIBE_RESULT_COLS, pRsp);
    }
  }
  (void)blockDataDestroy(pBlock);
  return code;
}

static int32_t execResetQueryCache() { return catalogClearCache(); }

static int32_t buildCreateDBResultDataBlock(SSDataBlock** pOutput) {
  QRY_PARAM_CHECK(pOutput);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_COLS, 1);
  code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_DB_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    (void)blockDataDestroy(pBlock);
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

static int32_t buildRetension(SArray* pRetension, char** ppRetentions) {
  size_t size = taosArrayGetSize(pRetension);
  if (size == 0) {
    *ppRetentions = NULL;
    return TSDB_CODE_SUCCESS;
  }

  const int lMaxLen = 128;
  char* p1 = taosMemoryCalloc(1, lMaxLen);
  if (NULL == p1) {
    return terrno;
  }
  int32_t len = 0;

  for (int32_t i = 0; i < size; ++i) {
    SRetention* p = TARRAY_GET_ELEM(pRetension, i);
    int64_t     v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    int64_t     v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    if (i == 0) {
      len += tsnprintf(p1 + len, lMaxLen - len, "-:%" PRId64 "%c", v2, p->keepUnit);
    } else {
      len += tsnprintf(p1 + len, lMaxLen - len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
    }

    if (i < size - 1) {
      len += tsnprintf(p1 + len, lMaxLen - len, ",");
    }
  }

  *ppRetentions = p1;
  return TSDB_CODE_SUCCESS;
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

static const char* encryptAlgorithmStr(int8_t encryptAlgorithm) {
  switch (encryptAlgorithm) {
    case TSDB_ENCRYPT_ALGO_NONE:
      return TSDB_ENCRYPT_ALGO_NONE_STR;
    case TSDB_ENCRYPT_ALGO_SM4:
      return TSDB_ENCRYPT_ALGO_SM4_STR;
    default:
      break;
  }
  return TSDB_CACHE_MODEL_NONE_STR;
}

int32_t formatDurationOrKeep(char* buffer, int64_t bufSize, int32_t timeInMinutes) {
    if (buffer == NULL || bufSize <= 0) {
        return 0;
    }
    int32_t len = 0;
    if (timeInMinutes % 1440 == 0) {
      int32_t days = timeInMinutes / 1440;
      len = tsnprintf(buffer, bufSize, "%dd", days);
    } else if (timeInMinutes % 60 == 0) {
      int32_t hours = timeInMinutes / 60;
      len = tsnprintf(buffer, bufSize, "%dh", hours);
    } else {
      len = tsnprintf(buffer, bufSize, "%dm", timeInMinutes);
    }
    return len;
}

static int32_t setCreateDBResultIntoDataBlock(SSDataBlock* pBlock, char* dbName, char* dbFName, SDbCfgInfo* pCfg) {
  QRY_ERR_RET(blockDataEnsureCapacity(pBlock, 1));
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_DB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, dbName);
  COL_DATA_SET_VAL_AND_CHECK(pCol1, 0, buf1, false);

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

  char* pRetentions = NULL;
  QRY_ERR_RET(buildRetension(pCfg->pRetensions, &pRetentions));
  int32_t dbFNameLen = strlen(dbFName);
  int32_t hashPrefix = 0;
  if (pCfg->hashPrefix > 0) {
    hashPrefix = pCfg->hashPrefix - dbFNameLen - 1;
  } else if (pCfg->hashPrefix < 0) {
    hashPrefix = pCfg->hashPrefix + dbFNameLen + 1;
  }
  char durationStr[128] = {0};
  char keep0Str[128] = {0};
  char keep1Str[128] = {0};
  char keep2Str[128] = {0};

  int32_t lenDuration = formatDurationOrKeep(durationStr, sizeof(durationStr), pCfg->daysPerFile);
  int32_t lenKeep0 = formatDurationOrKeep(keep0Str, sizeof(keep0Str), pCfg->daysToKeep0);
  int32_t lenKeep1 = formatDurationOrKeep(keep1Str, sizeof(keep1Str), pCfg->daysToKeep1);
  int32_t lenKeep2 = formatDurationOrKeep(keep2Str, sizeof(keep2Str), pCfg->daysToKeep2);

  if (IS_SYS_DBNAME(dbName)) {
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE, SHOW_CREATE_DB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE, "CREATE DATABASE `%s`", dbName);
  } else {
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE, SHOW_CREATE_DB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE,
                   "CREATE DATABASE `%s` BUFFER %d CACHESIZE %d CACHEMODEL '%s' COMP %d DURATION %s "
                   "WAL_FSYNC_PERIOD %d MAXROWS %d MINROWS %d STT_TRIGGER %d KEEP %s,%s,%s PAGES %d PAGESIZE %d "
                   "PRECISION '%s' REPLICA %d "
                   "WAL_LEVEL %d VGROUPS %d SINGLE_STABLE %d TABLE_PREFIX %d TABLE_SUFFIX %d TSDB_PAGESIZE %d "
                   "WAL_RETENTION_PERIOD %d WAL_RETENTION_SIZE %" PRId64
                   " KEEP_TIME_OFFSET %d ENCRYPT_ALGORITHM '%s' S3_CHUNKSIZE %d S3_KEEPLOCAL %dm S3_COMPACT %d",
                   dbName, pCfg->buffer, pCfg->cacheSize, cacheModelStr(pCfg->cacheLast), pCfg->compression,
                   durationStr,
                   pCfg->walFsyncPeriod, pCfg->maxRows, pCfg->minRows, pCfg->sstTrigger,
                   keep0Str, keep1Str, keep2Str,
                   pCfg->pages, pCfg->pageSize, prec,
                   pCfg->replications, pCfg->walLevel, pCfg->numOfVgroups, 1 == pCfg->numOfStables, hashPrefix,
                   pCfg->hashSuffix, pCfg->tsdbPageSize, pCfg->walRetentionPeriod, pCfg->walRetentionSize,
                   pCfg->keepTimeOffset, encryptAlgorithmStr(pCfg->encryptAlgorithm), pCfg->s3ChunkSize,
                   pCfg->s3KeepLocal, pCfg->s3Compact);

    if (pRetentions) {
      len += tsnprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_DB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE, " RETENTIONS %s", pRetentions);
    }
  }

  taosMemoryFree(pRetentions);

  (varDataLen(buf2)) = len;

  COL_DATA_SET_VAL_AND_CHECK(pCol2, 0, buf2, false);

  return TSDB_CODE_SUCCESS;
}

static int32_t execShowCreateDatabase(SShowCreateDatabaseStmt* pStmt, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildCreateDBResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = setCreateDBResultIntoDataBlock(pBlock, pStmt->dbName, pStmt->dbFName, pStmt->pCfg);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_CREATE_DB_RESULT_COLS, pRsp);
  }
  (void)blockDataDestroy(pBlock);
  return code;
}

static int32_t buildCreateTbResultDataBlock(SSDataBlock** pOutput) {
  QRY_PARAM_CHECK(pOutput);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD1_LEN, 1);
  code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_TB_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    (void)blockDataDestroy(pBlock);
  }
  return code;
}

static int32_t buildCreateViewResultDataBlock(SSDataBlock** pOutput) {
  QRY_PARAM_CHECK(pOutput);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_VIEW_RESULT_FIELD1_LEN, 1);
  code = blockDataAppendColInfo(pBlock, &infoData);
  if (TSDB_CODE_SUCCESS == code) {
    infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, SHOW_CREATE_VIEW_RESULT_FIELD2_LEN, 2);
    code = blockDataAppendColInfo(pBlock, &infoData);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pBlock;
  } else {
    (void)blockDataDestroy(pBlock);
  }
  return code;
}

void appendColumnFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
    SSchema* pSchema = pCfg->pSchemas + i;
#define LTYPE_LEN (32 + 60)  // 60 byte for compress info
    char type[LTYPE_LEN];
    snprintf(type, LTYPE_LEN, "%s", tDataTypes[pSchema->type].name);
    int typeLen  = strlen(type);
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type || TSDB_DATA_TYPE_VARBINARY == pSchema->type ||
        TSDB_DATA_TYPE_GEOMETRY == pSchema->type) {
      snprintf(type + typeLen, LTYPE_LEN - typeLen, "(%d)", (int32_t)(pSchema->bytes - VARSTR_HEADER_SIZE));
    } else if (TSDB_DATA_TYPE_NCHAR == pSchema->type) {
      snprintf(type + typeLen, LTYPE_LEN - typeLen, "(%d)",
               (int32_t)((pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }

    if (useCompress(pCfg->tableType) && pCfg->pSchemaExt) {
      typeLen = strlen(type);
      snprintf(type + typeLen, LTYPE_LEN - typeLen, " ENCODE \'%s\'",
               columnEncodeStr(COMPRESS_L1_TYPE_U32(pCfg->pSchemaExt[i].compress)));
      typeLen = strlen(type);
      snprintf(type + typeLen, LTYPE_LEN - typeLen, " COMPRESS \'%s\'",
               columnCompressStr(COMPRESS_L2_TYPE_U32(pCfg->pSchemaExt[i].compress)));
      typeLen = strlen(type);
      snprintf(type + typeLen, LTYPE_LEN - typeLen, " LEVEL \'%s\'",
               columnLevelStr(COMPRESS_L2_TYPE_LEVEL_U32(pCfg->pSchemaExt[i].compress)));
    }
    if (!(pSchema->flags & COL_IS_KEY)) {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len), "%s`%s` %s",
                       ((i > 0) ? ", " : ""), pSchema->name, type);
    } else {
      char* pk = "PRIMARY KEY";
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len), "%s`%s` %s %s",
                       ((i > 0) ? ", " : ""), pSchema->name, type, pk);
    }
  }
}

void appendTagFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    char     type[32];
    snprintf(type, sizeof(type), "%s", tDataTypes[pSchema->type].name);
    if (TSDB_DATA_TYPE_VARCHAR == pSchema->type || TSDB_DATA_TYPE_VARBINARY == pSchema->type ||
        TSDB_DATA_TYPE_GEOMETRY == pSchema->type) {
      snprintf(type + strlen(type), sizeof(type) - strlen(type), "(%d)", (int32_t)(pSchema->bytes - VARSTR_HEADER_SIZE));
    } else if (TSDB_DATA_TYPE_NCHAR == pSchema->type) {
      snprintf(type + strlen(type), sizeof(type) - strlen(type), "(%d)",
               (int32_t)((pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }

    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, sizeof(type) - (VARSTR_HEADER_SIZE + *len), "%s`%s` %s",
                     ((i > 0) ? ", " : ""), pSchema->name, type);
  }
}

void appendTagNameFields(char* buf, int32_t* len, STableCfg* pCfg) {
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     "%s`%s`", ((i > 0) ? ", " : ""), pSchema->name);
  }
}

int32_t appendTagValues(char* buf, int32_t* len, STableCfg* pCfg) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* pTagVals = NULL;
  STag*   pTag = (STag*)pCfg->pTags;

  if (NULL == pCfg->pTags || pCfg->numOfTags <= 0) {
    qError("tag missed in table cfg, pointer:%p, numOfTags:%d", pCfg->pTags, pCfg->numOfTags);
    return TSDB_CODE_APP_ERROR;
  }

  if (tTagIsJson(pTag)) {
    char* pJson = NULL;
    parseTagDatatoJson(pTag, &pJson);
    if (NULL == pJson) {
      qError("failed to parse tag to json, pJson is NULL");
      return terrno;
    }
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     "%s", pJson);
    taosMemoryFree(pJson);

    return TSDB_CODE_SUCCESS;
  }

  QRY_ERR_RET(tTagToValArray((const STag*)pCfg->pTags, &pTagVals));
  int16_t valueNum = taosArrayGetSize(pTagVals);
  int32_t num = 0;
  int32_t j = 0;
  for (int32_t i = 0; i < pCfg->numOfTags; ++i) {
    SSchema* pSchema = pCfg->pSchemas + pCfg->numOfColumns + i;
    if (i > 0) {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       ", ");
    }

    if (j >= valueNum) {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       "NULL");
      continue;
    }

    STagVal* pTagVal = (STagVal*)taosArrayGet(pTagVals, j);
    if (pSchema->colId > pTagVal->cid) {
      qError("tag value and column mismatch, schemaId:%d, valId:%d", pSchema->colId, pTagVal->cid);
      code = TSDB_CODE_APP_ERROR;
      TAOS_CHECK_ERRNO(code);
    } else if (pSchema->colId == pTagVal->cid) {
      char    type = pTagVal->type;
      int32_t tlen = 0;

      int64_t leftSize = SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len);
      if (leftSize <= 0) {
        qError("no enough space to store tag value, leftSize:%" PRId64, leftSize);
        code = TSDB_CODE_APP_ERROR;
        TAOS_CHECK_ERRNO(code);
      }
      if (IS_VAR_DATA_TYPE(type)) {
        code = dataConverToStr(buf + VARSTR_HEADER_SIZE + *len, leftSize, type, pTagVal->pData, pTagVal->nData, &tlen);
        TAOS_CHECK_ERRNO(code);
      } else {
        code = dataConverToStr(buf + VARSTR_HEADER_SIZE + *len, leftSize, type, &pTagVal->i64, tDataTypes[type].bytes, &tlen);
        TAOS_CHECK_ERRNO(code);
      }
      *len += tlen;
      j++;
    } else {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       "NULL");
    }
  }
_exit:
  taosArrayDestroy(pTagVals);

  return code;
}

void appendTableOptions(char* buf, int32_t* len, SDbCfgInfo* pDbCfg, STableCfg* pCfg) {
  if (pCfg->commentLen > 0) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " COMMENT '%s'", pCfg->pComment);
  } else if (0 == pCfg->commentLen) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " COMMENT ''");
  }

  if (NULL != pDbCfg->pRetensions && pCfg->watermark1 > 0) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " WATERMARK %" PRId64 "a", pCfg->watermark1);
    if (pCfg->watermark2 > 0) {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       ", %" PRId64 "a", pCfg->watermark2);
    }
  }

  if (NULL != pDbCfg->pRetensions && pCfg->delay1 > 0) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " MAX_DELAY %" PRId64 "a", pCfg->delay1);
    if (pCfg->delay2 > 0) {
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       ", %" PRId64 "a", pCfg->delay2);
    }
  }

  int32_t funcNum = taosArrayGetSize(pCfg->pFuncs);
  if (NULL != pDbCfg->pRetensions && funcNum > 0) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " ROLLUP(");
    for (int32_t i = 0; i < funcNum; ++i) {
      char* pFunc = taosArrayGet(pCfg->pFuncs, i);
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       "%s%s", ((i > 0) ? ", " : ""), pFunc);
    }
    *len +=
        snprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len), ")");
  }

  if (pCfg->ttl > 0) {
    *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                     " TTL %d", pCfg->ttl);
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
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len),
                       " SMA(");
      for (int32_t i = 0; i < pCfg->numOfColumns; ++i) {
        if (IS_BSMA_ON(pCfg->pSchemas + i)) {
          if (smaOn) {
            *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len,
                             SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len), ",`%s`",
                             (pCfg->pSchemas + i)->name);
          } else {
            smaOn = true;
            *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len,
                             SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + *len), "`%s`",
                             (pCfg->pSchemas + i)->name);
          }
        }
      }
      *len += tsnprintf(buf + VARSTR_HEADER_SIZE + *len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE, ")");
    }
  }
}

static int32_t setCreateTBResultIntoDataBlock(SSDataBlock* pBlock, SDbCfgInfo* pDbCfg, char* tbName, STableCfg* pCfg) {
  int32_t code = TSDB_CODE_SUCCESS;
  QRY_ERR_RET(blockDataEnsureCapacity(pBlock, 1));
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_TB_RESULT_FIELD1_LEN] = {0};
  STR_TO_VARSTR(buf1, tbName);
  QRY_ERR_RET(colDataSetVal(pCol1, 0, buf1, false));

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char*            buf2 = taosMemoryMalloc(SHOW_CREATE_TB_RESULT_FIELD2_LEN);
  if (NULL == buf2) {
    QRY_ERR_RET(terrno);
  }

  int32_t len = 0;

  if (TSDB_SUPER_TABLE == pCfg->tableType) {
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE, SHOW_CREATE_TB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE,
                    "CREATE STABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + len),
                    ") TAGS (");
    appendTagFields(buf2, &len, pCfg);
    len +=
        snprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + len), ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  } else if (TSDB_CHILD_TABLE == pCfg->tableType) {
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE, SHOW_CREATE_TB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE,
                    "CREATE TABLE `%s` USING `%s` (", tbName, pCfg->stbName);
    appendTagNameFields(buf2, &len, pCfg);
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + len),
                    ") TAGS (");
    code = appendTagValues(buf2, &len, pCfg);
    TAOS_CHECK_ERRNO(code);
    len +=
        snprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + len), ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  } else {
    len += tsnprintf(buf2 + VARSTR_HEADER_SIZE, SHOW_CREATE_TB_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE,
                    "CREATE TABLE `%s` (", tbName);
    appendColumnFields(buf2, &len, pCfg);
    len +=
        snprintf(buf2 + VARSTR_HEADER_SIZE + len, SHOW_CREATE_TB_RESULT_FIELD2_LEN - (VARSTR_HEADER_SIZE + len), ")");
    appendTableOptions(buf2, &len, pDbCfg, pCfg);
  }

  varDataLen(buf2) = (len > 65535) ? 65535 : len;

  code = colDataSetVal(pCol2, 0, buf2, false);
  TAOS_CHECK_ERRNO(code);

_exit:
  taosMemoryFree(buf2);

  return code;
}

static int32_t setCreateViewResultIntoDataBlock(SSDataBlock* pBlock, SShowCreateViewStmt* pStmt) {
  int32_t code = 0;
  QRY_ERR_RET(blockDataEnsureCapacity(pBlock, 1));
  pBlock->info.rows = 1;

  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char             buf1[SHOW_CREATE_VIEW_RESULT_FIELD1_LEN + 1] = {0};
  snprintf(varDataVal(buf1), TSDB_VIEW_FNAME_LEN + 4, "`%s`.`%s`", pStmt->dbName, pStmt->viewName);
  varDataSetLen(buf1, strlen(varDataVal(buf1)));
  QRY_ERR_RET(colDataSetVal(pCol1, 0, buf1, false));

  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  char*            buf2 = taosMemoryMalloc(SHOW_CREATE_VIEW_RESULT_FIELD2_LEN);
  if (NULL == buf2) {
    return terrno;
  }

  SViewMeta* pMeta = pStmt->pViewMeta;
  if(NULL == pMeta) {
    qError("exception: view meta is null");
    return TSDB_CODE_APP_ERROR;
  }
  snprintf(varDataVal(buf2), SHOW_CREATE_VIEW_RESULT_FIELD2_LEN - VARSTR_HEADER_SIZE, "CREATE VIEW `%s`.`%s` AS %s",
           pStmt->dbName, pStmt->viewName, pMeta->querySql);
  int32_t len = strlen(varDataVal(buf2));
  varDataLen(buf2) = (len > 65535) ? 65535 : len;
  code = colDataSetVal(pCol2, 0, buf2, false);
  taosMemoryFree(buf2);

  return code;
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
  (void)blockDataDestroy(pBlock);
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
    int32_t tmp = 0;
    code = taosStr2int32(value, &tmp);
    if (code) {
      qError("invalid value:%s, error:%s", value, tstrerror(code));
      return code;
    }
    code = schedulerUpdatePolicy(tmp);
  } else if (0 == strcasecmp(cmd, COMMAND_ENABLE_RESCHEDULE)) {
    int32_t tmp = 0;
    code = taosStr2int32(value, &tmp);
    if (code) {
      qError("invalid value:%s, error:%s", value, tstrerror(code));
      return code;
    }
    code = schedulerEnableReSchedule(tmp != 0);
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

  if (cfgSetItem(tsCfg, pStmt->config, pStmt->value, CFG_STYPE_ALTER_CMD, true)) {
    return terrno;
  }

  TAOS_CHECK_RETURN(taosCfgDynamicOptions(tsCfg, pStmt->config, false));

_return:

  return TSDB_CODE_SUCCESS;
}

static int32_t buildLocalVariablesResultDataBlock(SSDataBlock** pOutput) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (NULL == pBlock) {
    return terrno;
  }

  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(SHOW_LOCAL_VARIABLES_RESULT_COLS, sizeof(SColumnInfoData));
  if (NULL == pBlock->pDataBlock) {
    taosMemoryFree(pBlock);
    return terrno;
  }

  SColumnInfoData infoData = {0};

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD1_LEN;
  if (taosArrayPush(pBlock->pDataBlock, &infoData) == NULL) {
    goto _exit;
  }

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD2_LEN;
  if (taosArrayPush(pBlock->pDataBlock, &infoData) == NULL) {
    goto _exit;
  }

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_LOCAL_VARIABLES_RESULT_FIELD3_LEN;
  if (taosArrayPush(pBlock->pDataBlock, &infoData) == NULL) {
    goto _exit;
  }

  *pOutput = pBlock;

_exit:
  if (terrno != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBlock);
    taosArrayDestroy(pBlock->pDataBlock);
  }
  return terrno;
}

static int32_t execShowLocalVariables(SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildLocalVariablesResultDataBlock(&pBlock);
  if (TSDB_CODE_SUCCESS == code) {
    code = dumpConfToDataBlock(pBlock, 0);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRetrieveTableRsp(pBlock, SHOW_LOCAL_VARIABLES_RESULT_COLS, pRsp);
  }
  (void)blockDataDestroy(pBlock);
  return code;
}

static int32_t createSelectResultDataBlock(SNodeList* pProjects, SSDataBlock** pOutput) {
  QRY_PARAM_CHECK(pOutput);

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    return code;
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
    QRY_ERR_RET(blockDataAppendColInfo(pBlock, &infoData));
  }

  *pOutput = pBlock;
  return code;
}

int32_t buildSelectResultDataBlock(SNodeList* pProjects, SSDataBlock* pBlock) {
  QRY_ERR_RET(blockDataEnsureCapacity(pBlock, 1));

  int32_t index = 0;
  SNode*  pProj = NULL;
  FOREACH(pProj, pProjects) {
    if (QUERY_NODE_VALUE != nodeType(pProj)) {
      return TSDB_CODE_PAR_INVALID_SELECTED_EXPR;
    } else {
      if (((SValueNode*)pProj)->isNull) {
        QRY_ERR_RET(colDataSetVal(taosArrayGet(pBlock->pDataBlock, index++), 0, NULL, true));
      } else {
        QRY_ERR_RET(colDataSetVal(taosArrayGet(pBlock->pDataBlock, index++), 0,
                                  nodesGetValueFromNode((SValueNode*)pProj), false));
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
  (void)blockDataDestroy(pBlock);
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
  (void)blockDataDestroy(pBlock);
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
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}
