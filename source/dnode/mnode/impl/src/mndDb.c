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
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define TSDB_DB_VER_NUM 1
#define TSDB_DB_RESERVE_SIZE 64

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
  SSdbTable table = {.sdbType = SDB_DB,
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

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDb(SMnode *pMnode) {}

static SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, TSDB_DB_VER_NUM, sizeof(SDbObj) + TSDB_DB_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_FULL_DB_NAME_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid)
  SDB_SET_INT32(pRaw, dataPos, pDb->version)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheBlockSize)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.totalBlocks)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRowsPerFileBlock)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRowsPerFileBlock)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.commitTime)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.fsyncPeriod)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.quorum)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.update)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLastRow)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_DB_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_DB_VER_NUM) {
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
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->version)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.cacheBlockSize)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.totalBlocks)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysPerFile)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep0)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep1)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep2)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.minRowsPerFileBlock)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.maxRowsPerFileBlock)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.commitTime)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.fsyncPeriod)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.walLevel)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.precision)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.compression)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.replications)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.quorum)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.update)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->cfg.cacheLastRow)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_DB_RESERVE_SIZE)

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
  pOldDb->updateTime = pNewDb->createdTime;
  memcpy(&pOldDb->cfg, &pNewDb->cfg, sizeof(SDbCfg));
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

static int32_t mndCheckDbName(char *dbName, SUserObj *pUser) {
  char *pos = strstr(dbName, TS_PATH_DELIMITER);
  if (pos == NULL) {
    terrno = TSDB_CODE_MND_INVALID_DB;
    return -1;
  }

  int32_t acctId = atoi(dbName);
  if (acctId != pUser->acctId) {
    terrno = TSDB_CODE_MND_INVALID_DB_ACCT;
    return -1;
  }

  return 0;
}

static int32_t mndCheckDbCfg(SMnode *pMnode, SDbCfg *pCfg, char *errMsg, int32_t len) {
  if (pCfg->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCfg->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database cache block size option", len);
    return -1;
  }

  if (pCfg->totalBlocks < TSDB_MIN_TOTAL_BLOCKS || pCfg->totalBlocks > TSDB_MAX_TOTAL_BLOCKS) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database total blocks option", len);
    return -1;
  }

  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database days option", len);
    return -1;
  }

  if (pCfg->daysToKeep0 < pCfg->daysPerFile) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database days option", len);
    return -1;
  }

  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP || pCfg->daysToKeep0 > pCfg->daysToKeep1) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database keep0 option", len);
    return -1;
  }

  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP || pCfg->daysToKeep1 > pCfg->daysToKeep2) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database keep1 option", len);
    return -1;
  }

  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database keep2 option", len);
    return -1;
  }

  if (pCfg->minRowsPerFileBlock < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRowsPerFileBlock > TSDB_MAX_MIN_ROW_FBLOCK) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database minrows option", len);
    return -1;
  }

  if (pCfg->maxRowsPerFileBlock < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRowsPerFileBlock > TSDB_MAX_MAX_ROW_FBLOCK) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database maxrows option", len);
    return -1;
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database minrows option", len);
    return -1;
  }

  if (pCfg->commitTime < TSDB_MIN_COMMIT_TIME || pCfg->commitTime > TSDB_MAX_COMMIT_TIME) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database commit option", len);
    return -1;
  }

  if (pCfg->fsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->fsyncPeriod > TSDB_MAX_FSYNC_PERIOD) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database fsync option", len);
    return -1;
  }

  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database wal level option", len);
    return -1;
  }

  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid precision option", len);
    return -1;
  }

  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database compression option", len);
    return -1;
  }

  if (pCfg->replications < TSDB_MIN_DB_REPLICA_OPTION || pCfg->replications > TSDB_MAX_DB_REPLICA_OPTION) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database replication option", len);
    return -1;
  }

  if (pCfg->replications > mndGetDnodeSize(pMnode)) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database replication option", len);
    return -1;
  }

  if (pCfg->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCfg->quorum > TSDB_MAX_DB_QUORUM_OPTION) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database quorum option", len);
    return -1;
  }

  if (pCfg->quorum > pCfg->replications) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database quorum option", len);
    return -1;
  }

  if (pCfg->update < TSDB_MIN_DB_UPDATE || pCfg->update > TSDB_MAX_DB_UPDATE) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database update option", len);
    return -1;
  }

  if (pCfg->cacheLastRow < TSDB_MIN_DB_CACHE_LAST_ROW || pCfg->cacheLastRow > TSDB_MAX_DB_CACHE_LAST_ROW) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    tstrncpy(errMsg, "Invalid database cachelast option", len);
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

static void mndSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->cacheBlockSize < 0) pCfg->cacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
  if (pCfg->totalBlocks < 0) pCfg->totalBlocks = TSDB_DEFAULT_TOTAL_BLOCKS;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  if (pCfg->daysToKeep0 < 0) pCfg->daysToKeep0 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = TSDB_DEFAULT_KEEP;
  if (pCfg->minRowsPerFileBlock < 0) pCfg->minRowsPerFileBlock = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  if (pCfg->maxRowsPerFileBlock < 0) pCfg->maxRowsPerFileBlock = TSDB_DEFAULT_MAX_ROW_FBLOCK;
  if (pCfg->commitTime < 0) pCfg->commitTime = TSDB_DEFAULT_COMMIT_TIME;
  if (pCfg->fsyncPeriod < 0) pCfg->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  if (pCfg->walLevel < 0) pCfg->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  if (pCfg->precision < 0) pCfg->precision = TSDB_DEFAULT_PRECISION;
  if (pCfg->compression < 0) pCfg->compression = TSDB_DEFAULT_COMP_LEVEL;
  if (pCfg->replications < 0) pCfg->replications = TSDB_DEFAULT_DB_REPLICA_OPTION;
  if (pCfg->quorum < 0) pCfg->quorum = TSDB_DEFAULT_DB_QUORUM_OPTION;
  if (pCfg->update < 0) pCfg->update = TSDB_DEFAULT_DB_UPDATE_OPTION;
  if (pCfg->cacheLastRow < 0) pCfg->cacheLastRow = TSDB_DEFAULT_CACHE_LAST_ROW;
}

static int32_t mndCreateDb(SMnode *pMnode, SMnodeMsg *pMsg, SCreateDbMsg *pCreate, SUserObj *pUser) {
  SDbObj dbObj = {0};
  tstrncpy(dbObj.name, pCreate->db, TSDB_FULL_DB_NAME_LEN);
  tstrncpy(dbObj.acct, pUser->acct, TSDB_USER_LEN);
  dbObj.createdTime = taosGetTimestampMs();
  dbObj.updateTime = dbObj.createdTime;
  dbObj.uid = mndGenerateUid(dbObj.name, TSDB_FULL_DB_NAME_LEN);
  dbObj.cfg = (SDbCfg){.cacheBlockSize = pCreate->cacheBlockSize,
                       .totalBlocks = pCreate->totalBlocks,
                       .daysPerFile = pCreate->daysPerFile,
                       .daysToKeep0 = pCreate->daysToKeep0,
                       .daysToKeep1 = pCreate->daysToKeep1,
                       .daysToKeep2 = pCreate->daysToKeep2,
                       .minRowsPerFileBlock = pCreate->minRowsPerFileBlock,
                       .maxRowsPerFileBlock = pCreate->maxRowsPerFileBlock,
                       .fsyncPeriod = pCreate->fsyncPeriod,
                       .commitTime = pCreate->commitTime,
                       .precision = pCreate->precision,
                       .compression = pCreate->compression,
                       .walLevel = pCreate->walLevel,
                       .replications = pCreate->replications,
                       .quorum = pCreate->quorum,
                       .update = pCreate->update,
                       .cacheLastRow = pCreate->cacheLastRow};

  mndSetDefaultDbCfg(&dbObj.cfg);

  if (mndCheckDbName(dbObj.name, pUser) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  char errMsg[TSDB_ERROR_MSG_LEN] = {0};
  if (mndCheckDbCfg(pMnode, &dbObj.cfg, errMsg, TSDB_ERROR_MSG_LEN) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create db:%s", pTrans->id, pCreate->db);

  SSdbRaw *pRedoRaw = mndDbActionEncode(&dbObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING);

  SSdbRaw *pUndoRaw = mndDbActionEncode(&dbObj);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

  SSdbRaw *pCommitRaw = mndDbActionEncode(&dbObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateDbMsg(SMnodeMsg *pMsg) {
  SMnode       *pMnode = pMsg->pMnode;
  SCreateDbMsg *pCreate = pMsg->rpcMsg.pCont;

  pCreate->cacheBlockSize = htonl(pCreate->cacheBlockSize);
  pCreate->totalBlocks = htonl(pCreate->totalBlocks);
  pCreate->daysPerFile = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep0 = htonl(pCreate->daysToKeep0);
  pCreate->daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCreate->minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCreate->maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  pCreate->commitTime = htonl(pCreate->commitTime);
  pCreate->fsyncPeriod = htonl(pCreate->fsyncPeriod);

  mDebug("db:%s, start to create", pCreate->db);

  SDbObj *pDb = mndAcquireDb(pMnode, pCreate->db);
  if (pDb != NULL) {
    sdbRelease(pMnode->pSdb, pDb);
    if (pCreate->ignoreExist) {
      mDebug("db:%s, already exist, ignore exist is set", pCreate->db);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_DB_ALREADY_EXIST;
      mError("db:%s, failed to create since %s", pCreate->db, terrstr());
      return -1;
    }
  }

  SUserObj *pOperUser = mndAcquireUser(pMnode, pMsg->user);
  if (pOperUser == NULL) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  int32_t code = mndCreateDb(pMnode, pMsg, pCreate, pOperUser);
  mndReleaseUser(pMnode, pOperUser);

  if (code != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndSetDbCfgFromAlterDbMsg(SDbObj *pDb, SAlterDbMsg *pAlter) {
  bool changed = false;

  if (pAlter->totalBlocks >= 0 && pAlter->totalBlocks != pDb->cfg.totalBlocks) {
    pDb->cfg.totalBlocks = pAlter->totalBlocks;
    changed = true;
  }

  if (pAlter->daysToKeep0 >= 0 && pAlter->daysToKeep0 != pDb->cfg.daysToKeep0) {
    pDb->cfg.daysToKeep0 = pAlter->daysToKeep0;
    changed = true;
  }

  if (pAlter->daysToKeep1 >= 0 && pAlter->daysToKeep1 != pDb->cfg.daysToKeep1) {
    pDb->cfg.daysToKeep1 = pAlter->daysToKeep1;
    changed = true;
  }

  if (pAlter->daysToKeep2 >= 0 && pAlter->daysToKeep2 != pDb->cfg.daysToKeep2) {
    pDb->cfg.daysToKeep2 = pAlter->daysToKeep2;
    changed = true;
  }

  if (pAlter->fsyncPeriod >= 0 && pAlter->fsyncPeriod != pDb->cfg.fsyncPeriod) {
    pDb->cfg.fsyncPeriod = pAlter->fsyncPeriod;
    changed = true;
  }

  if (pAlter->walLevel >= 0 && pAlter->walLevel != pDb->cfg.walLevel) {
    pDb->cfg.walLevel = pAlter->walLevel;
    changed = true;
  }

  if (pAlter->quorum >= 0 && pAlter->quorum != pDb->cfg.quorum) {
    pDb->cfg.quorum = pAlter->quorum;
    changed = true;
  }

  if (pAlter->cacheLastRow >= 0 && pAlter->cacheLastRow != pDb->cfg.cacheLastRow) {
    pDb->cfg.cacheLastRow = pAlter->cacheLastRow;
    changed = true;
  }

  if (!changed) {
    terrno = TSDB_CODE_MND_DB_OPTION_UNCHANGED;
    return -1;
  }

  return 0;
}

static int32_t mndUpdateDb(SMnode *pMnode, SMnodeMsg *pMsg, SDbObj *pOldDb, SDbObj *pNewDb) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("db:%s, failed to update since %s", pOldDb->name, terrstr());
    return terrno;
  }

  mDebug("trans:%d, used to update db:%s", pTrans->id, pOldDb->name);

  SSdbRaw *pRedoRaw = mndDbActionEncode(pNewDb);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  SSdbRaw *pUndoRaw = mndDbActionEncode(pOldDb);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessAlterDbMsg(SMnodeMsg *pMsg) {
  SMnode      *pMnode = pMsg->pMnode;
  SAlterDbMsg *pAlter = pMsg->rpcMsg.pCont;
  pAlter->totalBlocks = htonl(pAlter->totalBlocks);
  pAlter->daysToKeep0 = htonl(pAlter->daysToKeep0);
  pAlter->daysToKeep1 = htonl(pAlter->daysToKeep1);
  pAlter->daysToKeep2 = htonl(pAlter->daysToKeep2);
  pAlter->fsyncPeriod = htonl(pAlter->fsyncPeriod);

  mDebug("db:%s, start to alter", pAlter->db);

  SDbObj *pDb = mndAcquireDb(pMnode, pAlter->db);
  if (pDb == NULL) {
    mError("db:%s, failed to alter since %s", pAlter->db, terrstr());
    return TSDB_CODE_MND_DB_NOT_EXIST;
  }

  SDbObj dbObj = {0};
  memcpy(&dbObj, pDb, sizeof(SDbObj));

  int32_t code = mndSetDbCfgFromAlterDbMsg(&dbObj, pAlter);
  if (code != 0) {
    mndReleaseDb(pMnode, pDb);
    mError("db:%s, failed to alter since %s", pAlter->db, tstrerror(code));
    return code;
  }

  dbObj.version++;
  code = mndUpdateDb(pMnode, pMsg, pDb, &dbObj);
  mndReleaseDb(pMnode, pDb);

  if (code != 0) {
    mError("db:%s, failed to alter since %s", pAlter->db, tstrerror(code));
    return code;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndDropDb(SMnode *pMnode, SMnodeMsg *pMsg, SDbObj *pDb) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("db:%s, failed to drop since %s", pDb->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop db:%s", pTrans->id, pDb->name);

  SSdbRaw *pRedoRaw = mndDbActionEncode(pDb);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING);

  SSdbRaw *pUndoRaw = mndDbActionEncode(pDb);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  SSdbRaw *pCommitRaw = mndDbActionEncode(pDb);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropDbMsg(SMnodeMsg *pMsg) {
  SMnode     *pMnode = pMsg->pMnode;
  SDropDbMsg *pDrop = pMsg->rpcMsg.pCont;

  mDebug("db:%s, start to drop", pDrop->db);

  SDbObj *pDb = mndAcquireDb(pMnode, pDrop->db);
  if (pDb == NULL) {
    if (pDrop->ignoreNotExists) {
      mDebug("db:%s, not exist, ignore not exist is set", pDrop->db);
      return TSDB_CODE_SUCCESS;
    } else {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      mError("db:%s, failed to drop since %s", pDrop->db, terrstr());
      return -1;
    }
  }

  int32_t code = mndDropDb(pMnode, pMsg, pDb);
  mndReleaseDb(pMnode, pDb);

  if (code != 0) {
    mError("db:%s, failed to drop since %s", pDrop->db, terrstr());
    return code;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessUseDbMsg(SMnodeMsg *pMsg) {
  SMnode    *pMnode = pMsg->pMnode;
  SUseDbMsg *pUse = pMsg->rpcMsg.pCont;

  SDbObj *pDb = mndAcquireDb(pMnode, pMsg->db);
  if (pDb != NULL) {
    strncpy(pMsg->db, pUse->db, TSDB_FULL_DB_NAME_LEN);
    mndReleaseDb(pMnode, pDb);
    return 0;
  } else {
    mError("db:%s, failed to process use db msg since %s", pMsg->db, terrstr());
    return -1;
  }
}

static int32_t mndProcessSyncDbMsg(SMnodeMsg *pMsg) {
  SMnode     *pMnode = pMsg->pMnode;
  SSyncDbMsg *pSync = pMsg->rpcMsg.pCont;

  SDbObj *pDb = mndAcquireDb(pMnode, pMsg->db);
  if (pDb == NULL) {
    mError("db:%s, failed to process sync db msg since %s", pMsg->db, terrstr());
    return -1;
  } else {
    mndReleaseDb(pMnode, pDb);
    return 0;
  }
}

static int32_t mndProcessCompactDbMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SCompactDbMsg *pCompact = pMsg->rpcMsg.pCont;

  SDbObj *pDb = mndAcquireDb(pMnode, pMsg->db);
  if (pDb == NULL) {
    mError("db:%s, failed to process compact db msg since %s", pMsg->db, terrstr());
    return -1;
  } else {
    mndReleaseDb(pMnode, pDb);
    return 0;
  }
}

static int32_t mndGetDbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

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
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

char *mnGetDbStr(char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;

  if (pos == NULL) {
    return src;
  }

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
    char *name = mnGetDbStr(pDb->name);
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

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextDb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}