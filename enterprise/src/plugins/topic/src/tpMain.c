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
#include "taosdef.h"
#include "tglobal.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeRead.h"
#include "mnodeWrite.h"

#define TP_SCHEMA_SQL_LEN 4096
#define TP_BINARY_LEN     16000

extern void *  tsDbSdb;
extern char *  mnodeGetDbStr(char *src);
extern int32_t mnodeProcessAlterDbMsg(SMnodeMsg *pMsg);
static int32_t mnodeGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeProcessCreateTpMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessAlterTpMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropTpMsg(SMnodeMsg *pMsg);
static void    mnodeCancelGetNextTp(void *pIter);

int32_t tpInit() {
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TP, mnodeProcessCreateTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TP, mnodeProcessAlterTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_TP, mnodeProcessDropTpMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_TP, mnodeGetTpMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_TP, mnodeRetrieveTps);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_TP, mnodeCancelGetNextTp);

  return 0;
}

void tpCleanUp() {}

void tpBuildCreateDbSql(char *sql, SCreateDbMsg *pCreate) {
  snprintf(sql, TP_SCHEMA_SQL_LEN,
           "create database if not exists %s replica %d days %d keep %d minrows %d maxrows %d cache %d blocks %d "
           "ctime %d wal %d "
           "fsync %d comp %d quorum %d cachelast %d precision us update 1",
           pCreate->db, pCreate->replications, pCreate->daysPerFile, pCreate->daysToKeep, pCreate->minRowsPerFileBlock,
           pCreate->maxRowsPerFileBlock, pCreate->cacheBlockSize, pCreate->totalBlocks, pCreate->commitTime,
           pCreate->walLevel, pCreate->fsyncPeriod, pCreate->compression, pCreate->quorum, pCreate->cacheLastRow);
}

static void tpBuildDropDbSql(char *sql, const char *topic) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "drop database %s", topic);
}

static void tpBuildCreateStableSql(char *sql, const char *topic) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "create table if not exists %s.partitions (offset timestamp, content binary(%d))",
           topic, TP_BINARY_LEN);
}

static void tpBuildCreateCtableSql(char *sql, const char *topic, int32_t tableId) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "create table if not exists %s.p%d using %s.ps tags(%d)", topic, tableId, topic,
           tableId);
}

static void tpBuildDropCtableSql(char *sql, const char *topic, int32_t tableId) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "drop table %s.p%d", topic, tableId);
}

static int32_t tpCreateTopicDb(TAOS *taos, SCreateDbMsg *pCreate) {
  char sql[TP_SCHEMA_SQL_LEN] = {0};
  tpBuildCreateDbSql(sql, pCreate);

  TAOS_RES *pSql = taos_query(taos, sql);
  int32_t   code = taos_errno(pSql);
  if (code == 0) {
    mInfo("topic:%s, db create success, code:%x", pCreate->db, code);
  } else {
    mError("topic:%s, failed to create db since %s, code:%x", pCreate->db, taos_errstr(pSql), code);
  }

  taos_free_result(pSql);
  return code;
}

static int32_t tpDropTopicDb(TAOS *taos, const char *topic) {
  char sql[TP_SCHEMA_SQL_LEN] = {0};
  tpBuildDropDbSql(sql, topic);

  TAOS_RES *pSql = taos_query(taos, sql);
  int32_t   code = taos_errno(pSql);
  if (code == 0 || code == TSDB_CODE_MND_INVALID_DB || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    code = 0;
    mInfo("topic:%s, db drop success, code:%x", topic, code);
  } else {
    mError("topic:%s, failed to drop db since %s, code:%x", topic, taos_errstr(pSql), code);
  }

  taos_free_result(pSql);
  return code;
}

static int32_t tpCreateTopicStable(TAOS *taos, const char *topic) {
  char sql[TP_SCHEMA_SQL_LEN] = {0};
  tpBuildCreateStableSql(sql, topic);

  TAOS_RES *pSql = taos_query(taos, sql);
  int32_t   code = taos_errno(pSql);
  if (code == 0) {
    mInfo("topic:%s, stable create success, code:%x", topic, code);
  } else {
    mError("topic:%s, failed to create stable since %s, code:%x", topic, taos_errstr(pSql), code);
  }

  taos_free_result(pSql);
  return code;
}

static int32_t tpCreateTopicCtable(TAOS *taos, const char *topic, int32_t partitions) {
  TAOS_RES *pSql = NULL;
  int32_t   code = 0;

  for (int32_t tableId = 1; tableId <= partitions; ++tableId) {
    char sql[TP_SCHEMA_SQL_LEN] = {0};
    tpBuildCreateCtableSql(sql, topic, tableId);

    pSql = taos_query(taos, sql);
    code = taos_errno(pSql);
    if (code == 0) {
      mInfo("topic:%s, table:%d create success, code:%x", topic, tableId, code);
    } else {
      mError("topic:%s, failed to create table:%d since %s, code:%x", topic, tableId, taos_errstr(pSql), code);
      break;
    }
  }
  taos_free_result(pSql);
  return code;
}

static int32_t tpDropTopicCtable(TAOS *taos, const char *topic, int32_t oldPartitions, int32_t partitions) {
  TAOS_RES *pSql = NULL;
  int32_t   code = 0;

  for (int32_t tableId = partitions + 1; tableId <= oldPartitions; ++tableId) {
    char sql[TP_SCHEMA_SQL_LEN] = {0};
    tpBuildDropCtableSql(sql, topic, tableId);

    pSql = taos_query(taos, sql);
    code = taos_errno(pSql);
    if (code == 0) {
      mInfo("topic:%s, table:%d drop success, code:%x", topic, tableId, code);
    } else {
      mError("topic:%s, failed to drop table:%d since %s, code:%x", topic, tableId, taos_errstr(pSql), code);
    }
  }

  taos_free_result(pSql);
  return 0;
}

static int32_t mnodeProcessCreateTpMsg(SMnodeMsg *pMsg) {
  SCreateDbMsg *pCreate = pMsg->rpcMsg.pCont;
  pCreate->maxTables = htonl(pCreate->maxTables);
  pCreate->cacheBlockSize = htonl(pCreate->cacheBlockSize);
  pCreate->totalBlocks = htonl(pCreate->totalBlocks);
  pCreate->daysPerFile = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep = htonl(pCreate->daysToKeep);
  pCreate->daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCreate->commitTime = htonl(pCreate->commitTime);
  pCreate->fsyncPeriod = htonl(pCreate->fsyncPeriod);
  pCreate->partitions = htons(pCreate->partitions);
  pCreate->minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCreate->maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);

  void *taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    return terrno;
  } else {
    mDebug("connect to database success");
  }

  int32_t code = tpCreateTopicDb(taos, pCreate);
  if (code != 0) {
    taos_close(taos);
    return code;
  }

  code = tpCreateTopicStable(taos, pCreate->db);
  if (code != 0) {
    taos_close(taos);
    return code;
  }

  code = tpCreateTopicCtable(taos, pCreate->db, pCreate->partitions);
  if (code != 0) {
    taos_close(taos);
    return code;
  }

  mInfo("topic:%s, create success, partitions:%d", pCreate->db, pCreate->partitions);
  return 0;
}

static int32_t mnodeProcessAlterTpMsg(SMnodeMsg *pMsg) {
  SAlterDbMsg *pAlter = pMsg->rpcMsg.pCont;
  int32_t      partitions = htons(pAlter->partitions);

  if (partitions < 1 || partitions > TSDB_MAX_DB_PARTITON_OPTION) {
    mError("invalid db option partitions:%d valid range: [%d, %d]", partitions, 1, TSDB_MAX_DB_PARTITON_OPTION);
    return TSDB_CODE_MND_INVALID_TOPIC_OPTION;
  }

  SDbObj *pDb = mnodeGetDb(pAlter->db);
  if (pDb == NULL || pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
    mError("topic:%s, failed to alter, invalid topic", pAlter->db);
    return TSDB_CODE_MND_INVALID_TOPIC;
  }

  int32_t oldPartitons = pDb->cfg.partitions;
  pDb->cfg.partitions = partitions;

  void *taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    return terrno;
  } else {
    mDebug("connect to database success");
  }

  
  tpCreateTopicCtable(taos, pAlter->db, partitions);
  tpDropTopicCtable(taos, pAlter->db, oldPartitons, partitions);

  mInfo("topic:%s, alter success, partitions:%d", pAlter->db, pAlter->partitions);
  taos_close(taos);

  return mnodeProcessAlterDbMsg(pMsg);
}

static int32_t mnodeProcessDropTpMsg(SMnodeMsg *pMsg) {
  SDropDbMsg *pDrop = pMsg->rpcMsg.pCont;

  void *taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    return terrno;
  } else {
    mDebug("connect to database success");
  }

  int32_t code = tpDropTopicDb(taos, pDrop->db);
  if (code != 0) {
    taos_close(taos);
    return code;
  }

  mInfo("topic:%s, drop success", pDrop->db);
  taos_close(taos);
  return 0;
}

static int32_t mnodeGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SSchema * pSchema = pMeta->schema;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "partitions");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->numOfRows = pUser->pAcct->acctInfo.numOfDbs;

  mnodeDecUserRef(pUser);
  return 0;
}

static int32_t mnodeRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SDbObj *  pDb = NULL;
  char *    pWrite;
  int32_t   cols = 0;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDb(pShow->pIter, &pDb);

    if (pDb == NULL) break;
    if (pDb->pAcct != pUser->pAcct || pDb->status != TSDB_DB_STATUS_READY || pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
      mnodeDecDbRef(pDb);
      continue;
    }

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
    *(int32_t *)pWrite = pDb->cfg.partitions;
    cols++;

    numOfRows++;
    mnodeDecDbRef(pDb);
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);

  mnodeDecUserRef(pUser);
  return numOfRows;
}

void mnodeCancelGetNextTp(void *pIter) {
  sdbFreeIter(tsDbSdb, pIter);
}

void tpUpdateTs(int32_t *seq, void *pMsg) {
  SSubmitMsg *pSubmit = pMsg;
  int32_t     numOfBlocks = htonl(pSubmit->numOfBlocks);
  int32_t     msgTotalLen = htonl(pSubmit->length);
  int32_t     blockOffset = sizeof(SSubmitMsg);
  int32_t     blocks = 0;

  while (blocks < numOfBlocks && blockOffset < msgTotalLen) {
    SSubmitBlk *pBlock = (SSubmitBlk *)((char *)pSubmit + blockOffset);
    int16_t     numOfRows = htons(pBlock->numOfRows);
    int32_t     blockTotalLen = htonl(pBlock->dataLen);
    int32_t     blockSchemaLen = htonl(pBlock->schemaLen);

    blockOffset += (sizeof(SSubmitBlk) + blockTotalLen + blockSchemaLen);
    blocks++;

    int32_t rowOffset = blockSchemaLen;
    int32_t rows = 0;
    while (rows < numOfRows && rowOffset < blockTotalLen) {
      SDataRow *pRow = (SDataRow *)((char *)pBlock->data + rowOffset);

      rowOffset += dataRowLen(pRow);
      rows++;

      (*seq)++;
      if ((*seq) > 1000000) seq = 0;
      dataRowTKey(pRow) = (1614873600000000L + *seq);
    }
  }
}