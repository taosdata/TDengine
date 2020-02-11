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

#define _DEFAULT_SOURCE
#include "os.h"
#include "mnode.h"
#include "mgmtUtil.h"
#include "mgmtTable.h"
#include "tschemautil.h"

bool mgmtTableCreateFromSuperTable(STabObj* pTableObj) {
  return pTableObj->tableType == TSDB_TABLE_TYPE_CREATE_FROM_STABLE;
}

bool mgmtIsSuperTable(STabObj* pTableObj) {
  return pTableObj->tableType == TSDB_TABLE_TYPE_SUPER_TABLE;
}

bool mgmtIsNormalTable(STabObj* pTableObj) {
  return !mgmtIsSuperTable(pTableObj);
}

/**
 * TODO: the tag offset value should be kept in memory to avoid dynamically calculating the value
 *
 * @param pTable
 * @param col
 * @param pTagColSchema
 * @return
 */
char* mgmtTableGetTag(STabObj* pTable, int32_t col, SSchema* pTagColSchema) {
  if (!mgmtTableCreateFromSuperTable(pTable)) {
    return NULL;
  }

  STabObj* pSuperTable = mgmtGetTable(pTable->pTagData);
  int32_t  offset = mgmtGetTagsLength(pSuperTable, col) + TSDB_TABLE_ID_LEN;
  assert(offset > 0);

  if (pTagColSchema != NULL) {
    *pTagColSchema = ((SSchema*)pSuperTable->schema)[pSuperTable->numOfColumns + col];
  }

  return (pTable->pTagData + offset);
}

int32_t mgmtGetTagsLength(STabObj* pSuperTable, int32_t col) {  // length before column col
  assert(mgmtIsSuperTable(pSuperTable) && col >= 0);

  int32_t len = 0;
  int32_t tagColumnIndexOffset = pSuperTable->numOfColumns;

  for (int32_t i = 0; i < pSuperTable->numOfTags && i < col; ++i) {
    len += ((SSchema*)pSuperTable->schema)[tagColumnIndexOffset + i].bytes;
  }

  return len;
}

bool mgmtCheckIsMonitorDB(char *db, char *monitordb) {
  char dbName[TSDB_DB_NAME_LEN + 1] = {0};
  extractDBName(db, dbName);

  size_t len = strlen(dbName);
  return (strncasecmp(dbName, monitordb, len) == 0 && len == strlen(monitordb));
}

int32_t mgmtCheckDBParams(SCreateDbMsg *pCreate) {
  if (pCreate->commitLog < 0 || pCreate->commitLog > 1) {
    mError("invalid db option commitLog: %d, only 0 or 1 allowed", pCreate->commitLog);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->replications < TSDB_REPLICA_MIN_NUM || pCreate->replications > TSDB_REPLICA_MAX_NUM) {
    mError("invalid db option replications: %d valid range: [%d, %d]", pCreate->replications, TSDB_REPLICA_MIN_NUM,
           TSDB_REPLICA_MAX_NUM);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->daysPerFile < TSDB_FILE_MIN_PARTITION_RANGE || pCreate->daysPerFile > TSDB_FILE_MAX_PARTITION_RANGE) {
    mError("invalid db option daysPerFile: %d valid range: [%d, %d]", pCreate->daysPerFile, TSDB_FILE_MIN_PARTITION_RANGE,
           TSDB_FILE_MAX_PARTITION_RANGE);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->daysToKeep1 > pCreate->daysToKeep2 || pCreate->daysToKeep2 > pCreate->daysToKeep) {
    mError("invalid db option daystokeep1: %d, daystokeep2: %d, daystokeep: %d", pCreate->daysToKeep1,
           pCreate->daysToKeep2, pCreate->daysToKeep);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->daysToKeep1 < TSDB_FILE_MIN_PARTITION_RANGE || pCreate->daysToKeep1 < pCreate->daysPerFile) {
    mError("invalid db option daystokeep: %d", pCreate->daysToKeep);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->rowsInFileBlock < TSDB_MIN_ROWS_IN_FILEBLOCK || pCreate->rowsInFileBlock > TSDB_MAX_ROWS_IN_FILEBLOCK) {
    mError("invalid db option rowsInFileBlock: %d valid range: [%d, %d]", pCreate->rowsInFileBlock,
           TSDB_MIN_ROWS_IN_FILEBLOCK, TSDB_MAX_ROWS_IN_FILEBLOCK);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCreate->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) {
    mError("invalid db option cacheBlockSize: %d valid range: [%d, %d]", pCreate->cacheBlockSize,
           TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->maxSessions < TSDB_MIN_TABLES_PER_VNODE || pCreate->maxSessions > TSDB_MAX_TABLES_PER_VNODE) {
    mError("invalid db option maxSessions: %d valid range: [%d, %d]", pCreate->maxSessions, TSDB_MIN_TABLES_PER_VNODE,
           TSDB_MAX_TABLES_PER_VNODE);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO) {
    mError("invalid db option timePrecision: %d valid value: [%d, %d]", pCreate->precision, TSDB_TIME_PRECISION_MILLI,
           TSDB_TIME_PRECISION_MICRO);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->cacheNumOfBlocks.fraction < TSDB_MIN_AVG_BLOCKS || pCreate->cacheNumOfBlocks.fraction > TSDB_MAX_AVG_BLOCKS) {
    mError("invalid db option ablocks: %f valid value: [%d, %d]", pCreate->cacheNumOfBlocks.fraction, 0, TSDB_MAX_AVG_BLOCKS);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->commitTime < TSDB_MIN_COMMIT_TIME_INTERVAL || pCreate->commitTime > TSDB_MAX_COMMIT_TIME_INTERVAL) {
    mError("invalid db option commitTime: %d valid range: [%d, %d]", pCreate->commitTime, TSDB_MIN_COMMIT_TIME_INTERVAL,
           TSDB_MAX_COMMIT_TIME_INTERVAL);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  if (pCreate->compression < TSDB_MIN_COMPRESSION_LEVEL || pCreate->compression > TSDB_MAX_COMPRESSION_LEVEL) {
    mError("invalid db option compression: %d valid range: [%d, %d]", pCreate->compression, TSDB_MIN_COMPRESSION_LEVEL,
           TSDB_MAX_COMPRESSION_LEVEL);
    return TSDB_CODE_INVALID_OPTION;
  }
  
  return TSDB_CODE_SUCCESS;
}
