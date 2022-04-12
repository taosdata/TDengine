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
#include "taoserror.h"
#include "tglobal.h"
#include "mndDef.h"
#include "tsdb.h"
#include "tfs.h"
#include "tdataformat.h"
#include "mnode.h"
#include "dnode.h"
#include "mndInt.h"
// #include "mnodeDb.h"
// #include "mnodeSdb.h"
// #include "mnodeShow.h"
// #include "mnodeUser.h"
// #include "mnodeRead.h"
// #include "mnodeWrite.h"

#define TP_SCHEMA_SQL_LEN 4096
#define TP_BINARY_LEN     16000

extern void *  tsDbSdb;
extern char *  mnodeGetDbStr(char *src);
extern int32_t mnodeProcessAlterDbMsg(SMnodeMsg *pMsg);
static int32_t tpGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t tpRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    tpCancelGetNextTp(void *pIter);
static int32_t tpRunInThread(int32_t msgType, SMnodeMsg *pMsg);
static int32_t tpProcessCreateTpMsg(SMnodeMsg *pMsg) { return tpRunInThread(TSDB_MSG_TYPE_CM_CREATE_TP, pMsg); }
static int32_t tpProcessAlterTpMsg(SMnodeMsg *pMsg) { return tpRunInThread(TSDB_MSG_TYPE_CM_ALTER_TP, pMsg); }
static int32_t tpProcessDropTpMsg(SMnodeMsg *pMsg) { return tpRunInThread(TSDB_MSG_TYPE_CM_DROP_TP, pMsg); }

int32_t tpInit() {
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TP, tpProcessCreateTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TP, tpProcessAlterTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_TP, tpProcessDropTpMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_TP, tpGetTpMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_TP, tpRetrieveTps);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_TP, tpCancelGetNextTp);

  return 0;
}

void tpCleanUp() {}

void tpBuildCreateDbSql(char *sql, SCreateDbMsg *pCreate) {
  int32_t maxTables = htonl(pCreate->maxTables);
  int32_t cacheBlockSize = htonl(pCreate->cacheBlockSize);
  int32_t totalBlocks = htonl(pCreate->totalBlocks);
  int32_t daysPerFile = htonl(pCreate->daysPerFile);
  int32_t daysToKeep = htonl(pCreate->daysToKeep0);
  int32_t daysToKeep1 = htonl(pCreate->daysToKeep1);
  int32_t daysToKeep2 = htonl(pCreate->daysToKeep2);
  int32_t commitTime = htonl(pCreate->commitTime);
  int32_t fsyncPeriod = htonl(pCreate->fsyncPeriod);
  int32_t partitions = htons(pCreate->partitions);
  int32_t minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  int32_t maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  int8_t  precision = pCreate->precision;
  int8_t  compression = pCreate->compression;
  int8_t  walLevel = pCreate->walLevel;
  int8_t  replications = pCreate->replications;
  int8_t  quorum = pCreate->quorum;
  int8_t  update = pCreate->update;
  int8_t  cacheLastRow = pCreate->cacheLastRow;

  if (maxTables < 0) maxTables = tsMaxTablePerVnode;
  if (cacheBlockSize < 0) cacheBlockSize = tsCacheBlockSize;
  if (totalBlocks < 0) totalBlocks = tsBlocksPerVnode;
  if (daysPerFile < 0) daysPerFile = tsDaysPerFile;
  if (daysToKeep < 0) daysToKeep = tsDaysToKeep;
  if (daysToKeep1 < 0) daysToKeep1 = daysToKeep;
  if (daysToKeep2 < 0) daysToKeep2 = daysToKeep;
  if (commitTime < 0) commitTime = tsCommitTime;
  if (fsyncPeriod < 0) fsyncPeriod = tsFsyncPeriod;
  if (partitions < 0) partitions = tsPartitons;
  if (minRowsPerFileBlock < 0) minRowsPerFileBlock = tsMinRowsInFileBlock;
  if (maxRowsPerFileBlock < 0) maxRowsPerFileBlock = tsMaxRowsInFileBlock;
  if (precision < 0) precision = tsTimePrecision;
  if (compression < 0) compression = tsCompression;
  if (walLevel < 0) walLevel = tsWAL;
  if (replications < 0) replications = tsReplications;
  if (quorum < 0) quorum = tsQuorum;
  if (update < 0) update = tsUpdate;
  if (cacheLastRow < 0) cacheLastRow = tsCacheLastRow;

  snprintf(sql, TP_SCHEMA_SQL_LEN,
           "create database if not exists %s replica %d days %d keep %d minrows %d maxrows %d cache %d blocks %d "
           "ctime %d wal %d "
           "fsync %d comp %d quorum %d cachelast %d precision 'us' update 0",
           mnodeGetDbStr(pCreate->db), replications, daysPerFile, daysToKeep, minRowsPerFileBlock, maxRowsPerFileBlock,
           cacheBlockSize, totalBlocks, commitTime, walLevel, fsyncPeriod, compression, quorum, cacheLastRow);
}

static void tpBuildDropDbSql(char *sql, const char *topic) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "drop database %s", topic);
}

static void tpBuildCreateStableSql(char *sql, const char *topic) {
  snprintf(sql, TP_SCHEMA_SQL_LEN, "create table if not exists %s.ps (off timestamp, ts timestamp, content binary(%d)) tags(pid int)",
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

  if (pSql != NULL) taos_free_result(pSql);
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

  if (pSql != NULL) taos_free_result(pSql);
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

  if (pSql != NULL) taos_free_result(pSql);
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
  if (pSql != NULL) taos_free_result(pSql);
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

  if (pSql != NULL) taos_free_result(pSql);
  return 0;
}

static void *tpProcessCreateTp(void *param) {
  SMnodeMsg *   pMsg = param;
  void *        taos = NULL;
  SDbObj *      pDb = NULL;
  int32_t       code = 0;
  char db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN] = {0};

  SCreateDbMsg *pCreate = pMsg->rpcMsg.pCont;
  pDb = mnodeGetDb(pCreate->db);

  tstrncpy(db, pCreate->db, sizeof(db));
  if (pDb != NULL) {
    if (pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
      mError("topic:%s, db already exist but type is not topic", db);
      code = TSDB_CODE_MND_DB_ALREADY_EXIST;
      mnodeDecDbRef(pDb);
      pDb = NULL;
      goto ctp_over;
    }

    if (pCreate->ignoreExist) {
      mDebug("topic:%s, db already exist, ignore exist is set", db);
      mnodeDecDbRef(pDb);
      pDb = NULL;
    } else {
      mError("topic:%s, db already exist, ignore exist not set", db);
      code = TSDB_CODE_MND_TOPIC_ALREADY_EXIST;
      mnodeDecDbRef(pDb);
      pDb = NULL;
      goto ctp_over;
    }
  }

  int16_t partitions = htons(pCreate->partitions);
  mDebug("topic:%s, start to create, partitions:%d", db, partitions);

  if (partitions == -1) {
    partitions = TSDB_DEFAULT_DB_PARTITON_OPTION;
  }

  if (partitions < 0 || partitions > TSDB_MAX_DB_PARTITON_OPTION) {
    mError("invalid db option partitions:%d valid range: [%d, %d]", partitions, 0, TSDB_MAX_DB_PARTITON_OPTION);
    code = TSDB_CODE_MND_INVALID_TOPIC_PARTITONS;
    goto ctp_over;
  }

  taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    code = terrno;
    goto ctp_over;
  } else {
    mDebug("connect to database success");
  }

  code = tpCreateTopicDb(taos, pCreate);
  if (code != 0) {
    goto ctp_over;
  }

  pDb = mnodeGetDb(pCreate->db);
  if (pDb != NULL) pDb->cfg.dbType = TSDB_DB_TYPE_TOPIC;

  if (partitions != 0) {
    code = tpCreateTopicStable(taos, mnodeGetDbStr(pCreate->db));
    if (code != 0) {
      goto ctp_over;
    }
  }

  code = tpCreateTopicCtable(taos, mnodeGetDbStr(pCreate->db), partitions);
  if (code != 0) {
    goto ctp_over;
  }

  mInfo("topic:%s, all table created", db);

ctp_over:
  taos_close(taos);
  if (pDb != NULL) {
    pDb->cfg.dbType = TSDB_DB_TYPE_DEFAULT;
    mnodeDecDbRef(pDb);
  }

  if (code == 0) {
    pCreate->dbType = TSDB_DB_TYPE_TOPIC;
    pCreate->partitions = htons(partitions);
    code = mnodeProcessAlterDbMsg(pMsg);
  }

  dnodeSendRpcMWriteRsp(pMsg, code);

  mDebug("topic:%s, create topic thread finished", db);
  return NULL;
}

static void *tpProcessAlterTp(void *param) {
  SMnodeMsg *  pMsg = param;
  void *       taos = NULL;
  SDbObj *     pDb = NULL;
  SAlterDbMsg *pAlter = pMsg->rpcMsg.pCont;
  int32_t      partitions = htons(pAlter->partitions);
  int32_t      code = 0;
  char db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN] = {0};

  tstrncpy(db, pAlter->db, sizeof(db));

  pDb = mnodeGetDb(pAlter->db);
  if (pDb == NULL || pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
    mError("topic:%s, failed to alter, invalid topic", db);
    code = TSDB_CODE_MND_INVALID_TOPIC;
    goto atp_over;
  }

  if (partitions < 0 || partitions > TSDB_MAX_DB_PARTITON_OPTION) {
    mError("invalid db option partitions:%d valid range: [%d, %d]", partitions, 0, TSDB_MAX_DB_PARTITON_OPTION);
    code = TSDB_CODE_MND_INVALID_TOPIC_PARTITONS;
    goto atp_over;
  }

  int32_t oldPartitons = pDb->cfg.partitions;
  pDb->cfg.partitions = partitions;
  mDebug("topic:%s, start to alter, partitions:%d, old:%d", db, partitions, oldPartitons);

  taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    code = terrno;
    goto atp_over;
  } else {
    mDebug("connect to database success");
  }

  if (partitions != 0) {
    code = tpCreateTopicStable(taos, mnodeGetDbStr(pAlter->db));
    if (code != 0) {
      goto atp_over;
    }
  }

  tpCreateTopicCtable(taos, mnodeGetDbStr(pAlter->db), partitions);
  tpDropTopicCtable(taos, mnodeGetDbStr(pAlter->db), oldPartitons, partitions);

  mInfo("topic:%s, all table updated, partitions:%d", db, partitions);

atp_over:
  taos_close(taos);
  if (pDb != NULL) {
    mnodeDecDbRef(pDb);
  }

  if (code == 0) {
    pAlter->dbType = TSDB_DB_TYPE_TOPIC;
    code = mnodeProcessAlterDbMsg(pMsg);
  }

  dnodeSendRpcMWriteRsp(pMsg, code);

  mDebug("topic:%s, alter topic thread finished", db);
  return NULL;
}

static void *tpProcessDropTp(void *param) {
  SMnodeMsg * pMsg = param;
  SDbObj *    pDb = NULL;
  int32_t     code = 0;
  SDropDbMsg *pDrop = pMsg->rpcMsg.pCont;
  void *      taos = NULL;

  //not change msg protocal between client/server, actually, db max length is (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN)
  //just make runtime happy
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tstrncpy(db, pDrop->db, sizeof(db));

  mDebug("topic:%s, start to drop", db);
  pDb = mnodeGetDb(pDrop->db);
  if (pDb == NULL) {
    if (pDrop->ignoreNotExists) {
      mDebug("topic:%s, tp already exist, ignore exist is set", db);
      goto dtp_over;
    } 
  }

  if (pDb == NULL || pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
    mError("topic:%s, failed to drop, invalid topic", db);
    code = TSDB_CODE_MND_INVALID_TOPIC;
    goto dtp_over;
  }

  taos = taos_connect(NULL, "monitor", tsInternalPass, "", 0);
  if (taos == NULL) {
    mError("failed to connect to database, reason:%s", tstrerror(terrno));
    code = terrno;
    goto dtp_over;
  } else {
    mDebug("connect to database success");
  }

  code = tpDropTopicDb(taos, mnodeGetDbStr(pDrop->db));
  if (code != 0) {
    goto dtp_over;
  }

  mInfo("topic:%s, drop success", db);

dtp_over:
  taos_close(taos);
  if (pDb != NULL) {
    mnodeDecDbRef(pDb);
  }

  dnodeSendRpcMWriteRsp(pMsg, code);

  mDebug("topic:%s, drop topic thread finished", db);
  return NULL;
}

static int32_t tpRunInThread(int32_t msgType, SMnodeMsg *pMsg) {
  mDebug("msg:%s will be processd in topic thread", taosMsg[msgType]);

  void *(*msgFp)(void *arg) = NULL;
  if (msgType == TSDB_MSG_TYPE_CM_CREATE_TP) {
    msgFp = tpProcessCreateTp;
  } else if (msgType == TSDB_MSG_TYPE_CM_ALTER_TP) {
    msgFp = tpProcessAlterTp;
  } else if (msgType == TSDB_MSG_TYPE_CM_DROP_TP) {
    msgFp = tpProcessDropTp;
  } else {
  }

  if (msgFp == NULL) {
    mDebug("msg:%s won't be processed in topic thread", taosMsg[msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  TdThread       threadID;
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&threadID, &thattr, msgFp, pMsg) != 0) {
    mError("failed to topic thread since %s", strerror(errno));
    return TSDB_CODE_MND_APP_ERROR;
  }
  mTrace("topic thread is created to process msg:%s", taosMsg[msgType]);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t tpGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
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

static int32_t tpRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
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

void tpCancelGetNextTp(void *pIter) { sdbFreeIter(tsDbSdb, pIter); }

void tpUpdateTs(int32_t vgId, int64_t *seq, void *pMsg) {
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
    int64_t sec = (int64_t)taosGetTimestampSec() * 1000000L;
    while (rows < numOfRows && rowOffset < blockTotalLen) {
      SMemRow *pRow = (SMemRow *)((char *)pBlock->data + rowOffset);

      rowOffset += memRowTLen(pRow);
      rows++;

      if ((*seq)++ < sec) {
        *seq = sec;
      }

      memRowSetTKey(pRow, *seq);
      mTrace("vgId:%d, sec:%" PRId64 ", seq:%" PRId64, vgId, sec, *seq);
    }
  }
}
