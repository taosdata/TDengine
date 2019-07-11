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

#include "mgmt.h"
#include <arpa/inet.h>
#include "tschemautil.h"

void *dbSdb = NULL;
int   tsDbUpdateSize;

void *(*mgmtDbActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);
void *mgmtDbActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtDbActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtDbActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDbActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtDbActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtDbActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDbActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDbActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDbActionReset(void *row, char *str, int size, int *ssize);
void *mgmtDbActionDestroy(void *row, char *str, int size, int *ssize);

void mgmtDbActionInit() {
  mgmtDbActionFp[SDB_TYPE_INSERT] = mgmtDbActionInsert;
  mgmtDbActionFp[SDB_TYPE_DELETE] = mgmtDbActionDelete;
  mgmtDbActionFp[SDB_TYPE_UPDATE] = mgmtDbActionUpdate;
  mgmtDbActionFp[SDB_TYPE_ENCODE] = mgmtDbActionEncode;
  mgmtDbActionFp[SDB_TYPE_DECODE] = mgmtDbActionDecode;
  mgmtDbActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtDbActionBeforeBatchUpdate;
  mgmtDbActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtDbActionBatchUpdate;
  mgmtDbActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtDbActionAfterBatchUpdate;
  mgmtDbActionFp[SDB_TYPE_RESET] = mgmtDbActionReset;
  mgmtDbActionFp[SDB_TYPE_DESTROY] = mgmtDbActionDestroy;
}

void *mgmtDbAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtDbActionFp[action] != NULL) {
    return (*(mgmtDbActionFp[action]))(row, str, size, ssize);
  }
  return NULL;
}

void mgmtGetAcctStr(char *src, char *dest) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  while ((pos != NULL) && (*src != *pos)) {
    *dest = *src;
    src++;
    dest++;
  }

  *dest = 0;
}

int mgmtInitDbs() {
  void *  pNode = NULL;
  SDbObj *pDb = NULL;

  mgmtDbActionInit();

  dbSdb = sdbOpenTable(tsMaxDbs, sizeof(SDbObj), "db", SDB_KEYTYPE_STRING, mgmtDirectory, mgmtDbAction);
  if (dbSdb == NULL) {
    mError("failed to init db data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(dbSdb, pNode, (void **)&pDb);
    if (pDb == NULL) break;

    pDb->pHead = NULL;
    pDb->pTail = NULL;
    pDb->prev = NULL;
    pDb->next = NULL;
    pDb->numOfTables = 0;
    pDb->numOfVgroups = 0;
    pDb->numOfMetrics = 0;
    pDb->vgStatus = TSDB_VG_STATUS_READY;
    pDb->vgTimer = NULL;
    pDb->pMetric = NULL;
    mgmtAddDbIntoAcct(&acctObj, pDb);
  }

  SDbObj tObj;
  tsDbUpdateSize = tObj.updateEnd - (char *)&tObj;

  mTrace("db data is initialized");
  return 0;
}

SDbObj *mgmtGetDb(char *db) { return (SDbObj *)sdbGetRow(dbSdb, db); }

SDbObj *mgmtGetDbByMeterId(char *meterId) {
  char db[TSDB_METER_ID_LEN], *pos;

  pos = strstr(meterId, TS_PATH_DELIMITER);
  pos = strstr(pos + 1, TS_PATH_DELIMITER);
  memset(db, 0, sizeof(db));
  strncpy(db, meterId, pos - meterId);

  return (SDbObj *)sdbGetRow(dbSdb, db);
}

int mgmtCheckDbParams(SCreateDbMsg *pCreate) {
  // assign default parameters
  if (pCreate->maxSessions < 0) pCreate->maxSessions = tsSessionsPerVnode;      //
  if (pCreate->cacheBlockSize < 0) pCreate->cacheBlockSize = tsCacheBlockSize;  //
  if (pCreate->daysPerFile < 0) pCreate->daysPerFile = tsDaysPerFile;           //
  if (pCreate->daysToKeep < 0) pCreate->daysToKeep = tsDaysToKeep;              //
  if (pCreate->daysToKeep1 < 0) pCreate->daysToKeep1 = pCreate->daysToKeep;     //
  if (pCreate->daysToKeep2 < 0) pCreate->daysToKeep2 = pCreate->daysToKeep;     //
  if (pCreate->commitTime < 0) pCreate->commitTime = tsCommitTime;              //
  if (pCreate->compression < 0) pCreate->compression = tsCompression;           //
  if (pCreate->commitLog < 0) pCreate->commitLog = tsCommitLog;
  if (pCreate->replications < 0) pCreate->replications = 1;                                               //
  if (pCreate->rowsInFileBlock < 0) pCreate->rowsInFileBlock = tsRowsInFileBlock;                         //
  if (pCreate->cacheNumOfBlocks.fraction < 0) pCreate->cacheNumOfBlocks.fraction = tsAverageCacheBlocks;  //

  if (pCreate->replications != 1) {
    mTrace("invalid db option replications: %d", pCreate->replications);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->commitLog < 0 || pCreate->commitLog > 1) {
    mTrace("invalid db option commitLog: %d", pCreate->commitLog);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->daysPerFile < TSDB_FILE_MIN_PARTITION_RANGE || pCreate->daysPerFile > TSDB_FILE_MAX_PARTITION_RANGE) {
    mTrace("invalid db option daysPerFile: %d valid range: %d--%d", pCreate->daysPerFile, TSDB_FILE_MIN_PARTITION_RANGE,
           TSDB_FILE_MAX_PARTITION_RANGE);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->daysToKeep1 > pCreate->daysToKeep2 || pCreate->daysToKeep2 > pCreate->daysToKeep) {
    mTrace("invalid db option daystokeep1: %d, daystokeep2: %d, daystokeep: %d", pCreate->daysToKeep1,
           pCreate->daysToKeep2, pCreate->daysToKeep);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->daysToKeep1 < TSDB_FILE_MIN_PARTITION_RANGE || pCreate->daysToKeep1 < pCreate->daysPerFile) {
    mTrace("invalid db option daystokeep: %d", pCreate->daysToKeep);
    return TSDB_CODE_INVALID_OPTION;
  }
  if (pCreate->rowsInFileBlock < TSDB_MIN_ROWS_IN_FILEBLOCK || pCreate->rowsInFileBlock > TSDB_MAX_ROWS_IN_FILEBLOCK) {
    mTrace("invalid db option rowsInFileBlock: %d valid range: %d--%d", pCreate->rowsInFileBlock,
           TSDB_MIN_ROWS_IN_FILEBLOCK, TSDB_MAX_ROWS_IN_FILEBLOCK);
    return TSDB_CODE_INVALID_OPTION;
  }
  if (pCreate->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCreate->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) {
    mTrace("invalid db option cacheBlockSize: %d valid range: %d--%d", pCreate->cacheBlockSize,
           TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return TSDB_CODE_INVALID_OPTION;
  }
  if (pCreate->maxSessions < TSDB_MIN_TABLES_PER_VNODE || pCreate->maxSessions > TSDB_MAX_TABLES_PER_VNODE) {
    mTrace("invalid db option maxSessions: %d valid range: %d--%d", pCreate->maxSessions, TSDB_MIN_TABLES_PER_VNODE,
           TSDB_MAX_TABLES_PER_VNODE);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO) {
    mTrace("invalid db option timePrecision: %d valid value: %d,%d", pCreate->precision, TSDB_TIME_PRECISION_MILLI,
           TSDB_TIME_PRECISION_MICRO);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->cacheNumOfBlocks.fraction < 0 || pCreate->cacheNumOfBlocks.fraction > TSDB_MAX_AVG_BLOCKS) {
    mTrace("invalid db option ablocks: %d valid value: %d,%d", pCreate->precision, 0, TSDB_MAX_AVG_BLOCKS);
    return TSDB_CODE_INVALID_OPTION;
  } else {
    pCreate->cacheNumOfBlocks.totalBlocks = (int32_t)(pCreate->cacheNumOfBlocks.fraction * pCreate->maxSessions);
  }

  if (pCreate->cacheNumOfBlocks.totalBlocks > TSDB_MAX_CACHE_BLOCKS) {
    mTrace("invalid db option cacheNumOfBlocks: %d valid range: %d", pCreate->cacheNumOfBlocks.totalBlocks,
           TSDB_MAX_CACHE_BLOCKS);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->commitTime < TSDB_MIN_COMMIT_TIME_INTERVAL || pCreate->commitTime > TSDB_MAX_COMMIT_TIME_INTERVAL) {
    mTrace("invalid db option commitTime: %d valid range: %d-%d", pCreate->commitTime, TSDB_MIN_COMMIT_TIME_INTERVAL,
           TSDB_MAX_COMMIT_TIME_INTERVAL);
    return TSDB_CODE_INVALID_OPTION;
  }
  if (pCreate->compression > TSDB_MAX_COMPRESSION_LEVEL) {
    mTrace("invalid db option compression: %d", pCreate->compression, TSDB_MIN_COMMIT_TIME_INTERVAL,
           TSDB_MAX_COMMIT_TIME_INTERVAL);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCreate->blocksPerMeter < 0) pCreate->blocksPerMeter = tsNumOfBlocksPerMeter;
  if (pCreate->blocksPerMeter > pCreate->cacheNumOfBlocks.totalBlocks * 3 / 4) {
    pCreate->blocksPerMeter = pCreate->cacheNumOfBlocks.totalBlocks * 3 / 4;
  }
  if (pCreate->blocksPerMeter < 4) pCreate->blocksPerMeter = 4;

  pCreate->maxSessions++;

  return TSDB_CODE_SUCCESS;
}

int mgmtCreateDb(SAcctObj *pAcct, SCreateDbMsg *pCreate) {
  SDbObj *pDb;

  int numOfDbs = sdbGetNumOfRows(dbSdb);
  if (numOfDbs >= tsMaxDbs) {
    mWarn("numOfDbs:%d, exceed tsMaxDbs:%d", numOfDbs, tsMaxDbs);
    return TSDB_CODE_TOO_MANY_DATABSES;
  }

  pDb = (SDbObj *)sdbGetRow(dbSdb, pCreate->db);
  if (pDb != NULL) {
    return TSDB_CODE_DB_ALREADY_EXIST;
  }

  int code = mgmtCheckDbParams(pCreate);
  if (code != TSDB_CODE_SUCCESS) return code;

  assert(pCreate->daysToKeep1 <= pCreate->daysToKeep2 && pCreate->daysToKeep2 <= pCreate->daysToKeep);

  pDb = malloc(sizeof(SDbObj));
  memset(pDb, 0, sizeof(SDbObj));
  strcpy(pDb->name, pCreate->db);
  strcpy(pCreate->acct, pAcct->user);
  pDb->createdTime = taosGetTimestampMs();
  pDb->cfg = *pCreate;

  if (sdbInsertRow(dbSdb, pDb, 0) < 0) {
    code = TSDB_CODE_SDB_ERROR;
    tfree(pDb);
  }

  return code;
}

int mgmtUpdateDb(SDbObj *pDb) { return sdbUpdateRow(dbSdb, pDb, tsDbUpdateSize, 1); }

int mgmtSetDbDropping(SDbObj *pDb) {
  if (pDb->dropStatus == TSDB_DB_STATUS_DROP_FROM_SDB) return 0;

  SVgObj *pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    SDnodeObj *pDnode = &dnodeObj;
    if (pDnode == NULL) continue;

    SVnodeLoad *pVload = &pDnode->vload[pVgroup->vnodeGid[0].vnode];
    if (pVload->dropStatus != TSDB_VN_STATUS_DROPPING) {
      pVload->dropStatus = TSDB_VN_STATUS_DROPPING;
      mPrint("vnode:%d db:%s set to dropping status", pVgroup->vnodeGid[0].vnode, pDb->name);
    }
    mgmtSendFreeVnodeMsg(pVgroup->vnodeGid[0].vnode);
    pVgroup = pVgroup->next;
  }

  if (pDb->dropStatus == TSDB_DB_STATUS_DROPPING) return 0;

  pDb->dropStatus = TSDB_DB_STATUS_DROPPING;
  if (mgmtUpdateDb(pDb) < 0) {
    mError("db:%s drop failed, db sdb update error", pDb->name);
    return TSDB_CODE_SDB_ERROR;
  }

  mPrint("db:%s set to dropping status", pDb->name);
  return 0;
}

bool mgmtCheckDropDbFinished(SDbObj *pDb) {
  SVgObj *pVgroup = pDb->pHead;
  while (pVgroup) {
    SDnodeObj *pDnode = &dnodeObj;

    if (pDnode->status == TSDB_STATUS_OFFLINE) continue;

    SVnodeLoad *pVload = &pDnode->vload[pVgroup->vnodeGid[0].vnode];
    if (pVload->dropStatus == TSDB_VN_STATUS_DROPPING) {
      mTrace("dnode:0x%x vnode:%d db:%s wait dropping", pDnode->privateIp, pVgroup->vnodeGid[0].vnode, pDb->name);
      return false;
    }

    pVgroup = pVgroup->next;
  }

  mPrint("db:%s all vnodes drop finished", pDb->name);
  return true;
}

void mgmtDropDbFromSdb(SDbObj *pDb) {
  while (pDb->pHead) mgmtDropVgroup(pDb, pDb->pHead);

  STabObj *pMetric = pDb->pMetric;
  while (pMetric) {
    STabObj *pNext = pMetric->next;
    mgmtDropMeter(pDb, pMetric->meterId, 0);
    pMetric = pNext;
  }

  mPrint("db:%s all meters drop finished", pDb->name);
  sdbDeleteRow(dbSdb, pDb);
  mPrint("db:%s database drop finished", pDb->name);
}

int mgmtDropDb(SDbObj *pDb) {
  if (pDb->dropStatus == TSDB_DB_STATUS_DROPPING) {
    bool finished = mgmtCheckDropDbFinished(pDb);
    if (!finished) return TSDB_CODE_ACTION_IN_PROGRESS;

    // don't sync this action
    pDb->dropStatus = TSDB_DB_STATUS_DROP_FROM_SDB;
    mgmtDropDbFromSdb(pDb);
    return 0;
  } else {
    int code = mgmtSetDbDropping(pDb);
    if (code != 0) return code;
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
}

int mgmtDropDbByName(SAcctObj *pAcct, char *name) {
  SDbObj *pDb;
  pDb = (SDbObj *)sdbGetRow(dbSdb, name);
  if (pDb == NULL) {
    mWarn("db:%s is not there", name);
    // return TSDB_CODE_INVALID_DB;
    return 0;
  }

  if (taosCheckDbName(pDb->name, tsMonitorDbName)) return TSDB_CODE_MONITOR_DB_FORBEIDDEN;

  return mgmtDropDb(pDb);
}

void mgmtMonitorDbDrop(void *unused, void *unusedt) {
  void *  pNode = NULL;
  SDbObj *pDb = NULL;

  while (1) {
    pNode = sdbFetchRow(dbSdb, pNode, (void **)&pDb);
    if (pDb == NULL) break;
    if (pDb->dropStatus != TSDB_DB_STATUS_DROPPING) continue;
    mgmtDropDb(pDb);
    break;
  }
}

int mgmtAlterDb(SAcctObj *pAcct, SAlterDbMsg *pAlter) {
  SDbObj *pDb;
  int     code = TSDB_CODE_SUCCESS;

  pDb = (SDbObj *)sdbGetRow(dbSdb, pAlter->db);
  if (pDb == NULL) {
    mTrace("db:%s is not exist", pAlter->db);
    return TSDB_CODE_INVALID_DB;
  }

  if (pAlter->daysToKeep > 0) {
    mTrace("db:%s daysToKeep:%d change to %d", pDb->name, pDb->cfg.daysToKeep, pAlter->daysToKeep);
    pDb->cfg.daysToKeep = pAlter->daysToKeep;
  }

  if (sdbUpdateRow(dbSdb, pDb, tsDbUpdateSize, 1) < 0) {
    return TSDB_CODE_SDB_ERROR;
  }

  return code;
}

int mgmtUseDb(SConnObj *pConn, char *name) {
  SDbObj *pDb;
  int     code = TSDB_CODE_INVALID_DB;

  pDb = mgmtGetDb(name);
  if (pDb) {
    pConn->pDb = pDb;
    code = 0;
  }

  return code;
}

int mgmtAddVgroupIntoDb(SDbObj *pDb, SVgObj *pVgroup) {
  pVgroup->next = pDb->pHead;
  pVgroup->prev = NULL;

  if (pDb->pHead) pDb->pHead->prev = pVgroup;

  if (pDb->pTail == NULL) pDb->pTail = pVgroup;

  pDb->pHead = pVgroup;
  pDb->numOfVgroups++;

  return 0;
}

int mgmtAddVgroupIntoDbTail(SDbObj *pDb, SVgObj *pVgroup) {
  pVgroup->next = NULL;
  pVgroup->prev = pDb->pTail;

  if (pDb->pTail) pDb->pTail->next = pVgroup;

  if (pDb->pHead == NULL) pDb->pHead = pVgroup;

  pDb->pTail = pVgroup;
  pDb->numOfVgroups++;

  return 0;
}

int mgmtRemoveVgroupFromDb(SDbObj *pDb, SVgObj *pVgroup) {
  if (pVgroup->prev) pVgroup->prev->next = pVgroup->next;

  if (pVgroup->next) pVgroup->next->prev = pVgroup->prev;

  if (pVgroup->prev == NULL) pDb->pHead = pVgroup->next;

  if (pVgroup->next == NULL) pDb->pTail = pVgroup->prev;

  pDb->numOfVgroups--;

  return 0;
}

int mgmtMoveVgroupToTail(SDbObj *pDb, SVgObj *pVgroup) {
  mgmtRemoveVgroupFromDb(pDb, pVgroup);
  mgmtAddVgroupIntoDbTail(pDb, pVgroup);

  return 0;
}

int mgmtMoveVgroupToHead(SDbObj *pDb, SVgObj *pVgroup) {
  mgmtRemoveVgroupFromDb(pDb, pVgroup);
  mgmtAddVgroupIntoDb(pDb, pVgroup);

  return 0;
}

int mgmtAddMetricIntoDb(SDbObj *pDb, STabObj *pMetric) {
  pMetric->next = pDb->pMetric;
  pMetric->prev = NULL;

  if (pDb->pMetric) pDb->pMetric->prev = pMetric;

  pDb->pMetric = pMetric;
  pDb->numOfMetrics++;

  return 0;
}

int mgmtRemoveMetricFromDb(SDbObj *pDb, STabObj *pMetric) {
  if (pMetric->prev) pMetric->prev->next = pMetric->next;

  if (pMetric->next) pMetric->next->prev = pMetric->prev;

  if (pMetric->prev == NULL) pDb->pMetric = pMetric->next;

  pDb->numOfMetrics--;

  if (pMetric->pSkipList != NULL) {
    tSkipListDestroy(&pMetric->pSkipList);
  }
  return 0;
}

int mgmtShowTables(SAcctObj *pAcct, char *db) {
  int code;

  code = 0;

  return code;
}

void mgmtCleanUpDbs() { sdbCloseTable(dbSdb); }

int mgmtGetDbMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_DB_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "ntables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

#ifndef __CLOUD_VERSION__
  if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "vgroups");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

#ifndef __CLOUD_VERSION__
  if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "replica");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "days");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

  pShow->bytes[cols] = 24;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep1,keep2,keep(D)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

#ifndef __CLOUD_VERSION__
  if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "tables");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "rows");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "cache(b)");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
    strcpy(pSchema[cols].name, "ablocks");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "tblocks");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "ctime(s)");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "clog");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "comp");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

  pShow->bytes[cols] = 3;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "time precision");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  //  pShow->numOfRows = sdbGetNumOfRows (dbSdb);
  pShow->numOfRows = pConn->pAcct->acctInfo.numOfDbs;
  pShow->pNode = pConn->pAcct->pHead;

  return 0;
}

char *mgmtGetDbStr(char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);

  return ++pos;
}

int mgmtRetrieveDbs(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int     numOfRows = 0;
  SDbObj *pDb = NULL;
  char *  pWrite;
  int     cols = 0;

  while (numOfRows < rows) {
    pDb = (SDbObj *)pShow->pNode;
    if (pDb == NULL) break;
    pShow->pNode = (void *)pDb->next;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, mgmtGetDbStr(pDb->name));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->numOfTables;
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->numOfVgroups;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

#ifndef __CLOUD_VERSION__
    if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.replications;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.daysPerFile;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(pWrite, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep);
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pConn->pAcct->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.maxSessions - 1;  // table num can be created should minus 1
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.rowsInFileBlock;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.cacheBlockSize;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(float *)pWrite = (pDb->cfg.cacheNumOfBlocks.totalBlocks * 1.0 / (pDb->cfg.maxSessions - 1));
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.blocksPerMeter;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.commitTime;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.commitLog;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.compression;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *prec = (pDb->cfg.precision == TSDB_TIME_PRECISION_MILLI) ? TSDB_TIME_PRECISION_MILLI_STR
                                                                   : TSDB_TIME_PRECISION_MICRO_STR;
    strcpy(pWrite, prec);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pDb->dropStatus != TSDB_DB_STATUS_READY ? "dropping" : "ready");
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void *mgmtDbActionInsert(void *row, char *str, int size, int *ssize) {
  SDbObj *pDb = (SDbObj *)row;

  pDb->pHead = NULL;
  pDb->pTail = NULL;
  pDb->numOfVgroups = 0;
  pDb->numOfTables = 0;
  pDb->vgTimer = NULL;
  pDb->pMetric = NULL;
  mgmtAddDbIntoAcct(&acctObj, pDb);

  return NULL;
}
void *mgmtDbActionDelete(void *row, char *str, int size, int *ssize) {
  SDbObj *  pDb = (SDbObj *)row;
  SAcctObj *pAcct = &acctObj;
  mgmtRemoveDbFromAcct(pAcct, pDb);

  return NULL;
}
void *mgmtDbActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtDbActionReset(row, str, size, ssize);
}
void *mgmtDbActionEncode(void *row, char *str, int size, int *ssize) {
  SDbObj *pDb = (SDbObj *)row;
  int     tsize = pDb->updateEnd - (char *)pDb;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pDb, tsize);
    *ssize = tsize;
  }

  return NULL;
}
void *mgmtDbActionDecode(void *row, char *str, int size, int *ssize) {
  SDbObj *pDb = (SDbObj *)malloc(sizeof(SDbObj));
  if (pDb == NULL) return NULL;
  memset(pDb, 0, sizeof(SDbObj));

  int tsize = pDb->updateEnd - (char *)pDb;
  memcpy(pDb, str, tsize);

  return (void *)pDb;
}
void *mgmtDbActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtDbActionBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtDbActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtDbActionReset(void *row, char *str, int size, int *ssize) {
  SDbObj *pDb = (SDbObj *)row;
  int     tsize = pDb->updateEnd - (char *)pDb;
  memcpy(pDb, str, tsize);

  return NULL;
}
void *mgmtDbActionDestroy(void *row, char *str, int size, int *ssize) {
  tfree(row);
  return NULL;
}
