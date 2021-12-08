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
#include "mndDb.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndTrans.h"

#define TSDB_DB_VER 1

static SSdbRaw *mndDbActionEncode(SDbObj *pDb);
static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw);
static int32_t  mndDbActionInsert(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionDelete(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionUpdate(SSdb *pSdb, SDbObj *pOldDb, SDbObj *pNewDb);
static int32_t  mndProcessCreateDbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterDbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropDbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessUseDbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessSyncDbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessCompactDbMsg(SMnodeMsg *pMsg);
static int32_t  mndGetDbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveDbs(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextDb(SMnode *pMnode, void *pIter);

int32_t mndInitDb(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_USER,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndDbActionEncode,
                     .decodeFp = (SdbDecodeFp)mndDbActionDecode,
                     .insertFp = (SdbInsertFp)mndDbActionInsert,
                     .updateFp = (SdbUpdateFp)mndDbActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndDbActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_DB, mndProcessCreateDbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_DB, mndProcessAlterDbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_DB, mndProcessDropDbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_USE_DB, mndProcessUseDbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_SYNC_DB, mndProcessSyncDbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_COMPACT_DB, mndProcessCompactDbMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_DB, mndGetDbMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveDbs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextDb);

  return 0;
}

void mndCleanupDb(SMnode *pMnode) {}

static SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, TSDB_DB_VER, sizeof(SDbObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_FULL_DB_NAME_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheBlockSize)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.totalBlocks)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxTables)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRowsPerFileBlock)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRowsPerFileBlock)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.commitTime)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.fsyncPeriod)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.quorum)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.update)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLastRow)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.dbType)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_DB_VER) {
    mError("failed to decode db since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SDbObj));
  SDbObj  *pDb = sdbGetRowObj(pRow);
  if (pDb == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDb->name, TSDB_FULL_DB_NAME_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDb->acct, TSDB_USER_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->updateTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->uid)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.cacheBlockSize)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.totalBlocks)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.maxTables)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysPerFile)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep0)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep1)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep2)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.minRowsPerFileBlock)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.maxRowsPerFileBlock)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.commitTime)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.fsyncPeriod)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.precision)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.compression)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.walLevel)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.replications)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.quorum)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.update)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.cacheLastRow)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.dbType)

  return pRow;
}

static int32_t mndDbActionInsert(SSdb *pSdb, SDbObj *pDb) {
  mTrace("db:%s, perform insert action", pDb->name);
  return 0;
}

static int32_t mndDbActionDelete(SSdb *pSdb, SDbObj *pDb) {
  mTrace("db:%s, perform delete action", pDb->name);
  return 0;
}

static int32_t mndDbActionUpdate(SSdb *pSdb, SDbObj *pOldDb, SDbObj *pNewDb) {
  mTrace("db:%s, perform update action", pOldDb->name);
  memcpy(pOldDb, pNewDb, sizeof(SDbObj));
  return 0;
}

SDbObj *mndAcquireDb(SMnode *pMnode, char *db) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_DB, db);
}

void mndReleaseDb(SMnode *pMnode, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDb);
}

static int32_t mndProcessCreateDbMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessAlterDbMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropDbMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessUseDbMsg(SMnodeMsg *pMsg) {
  SMnode    *pMnode = pMsg->pMnode;
  SUseDbMsg *pUse = pMsg->rpcMsg.pCont;

  strncpy(pMsg->db, pUse->db, TSDB_FULL_DB_NAME_LEN);

  SDbObj *pDb = mndAcquireDb(pMnode, pMsg->db);
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
    return 0;
  } else {
    mError("db:%s, failed to process use db msg since %s", pMsg->db, terrstr());
    return -1;
  }
}

static int32_t mndProcessSyncDbMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessCompactDbMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndGetDbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "replica");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "quorum");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "days");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep0,keep1,keep2");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "cache(MB)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "blocks");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "minrows");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "maxrows");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "wallevel");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "fsync");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "comp");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "cachelast");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 3 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "precision");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "update");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_DB);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tableFname, mndShowStr(pShow->type));

  return 0;
}

char *mnodeGetDbStr(char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;

  return pos;
}

static int32_t mndRetrieveDbs(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SDbObj *pDb = NULL;
  char   *pWrite;
  int32_t cols = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_DB, pShow->pIter, (void **)&pDb);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *name = mnodeGetDbStr(pDb->name);
    if (name != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, pShow->bytes[cols]);
    } else {
      STR_TO_VARSTR(pWrite, "NULL");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.replications;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.quorum;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.daysPerFile;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char tmp[128] = {0};
    if (pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep1 || pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep2) {
      sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep0);
    } else {
      sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep0, pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2);
    }
    STR_WITH_SIZE_TO_VARSTR(pWrite, tmp, strlen(tmp));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.cacheBlockSize;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.totalBlocks;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.minRowsPerFileBlock;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.maxRowsPerFileBlock;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.walLevel;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.fsyncPeriod;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.compression;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.cacheLastRow;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *prec = NULL;
    switch (pDb->cfg.precision) {
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
        assert(false);
        break;
    }
    STR_WITH_SIZE_TO_VARSTR(pWrite, prec, 2);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.update;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pDb);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextDb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}