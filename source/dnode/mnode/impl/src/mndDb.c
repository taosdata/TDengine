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
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

#define TSDB_DB_VER_NUMBER 1
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

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DB, mndProcessCreateDbMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_DB, mndProcessAlterDbMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DB, mndProcessDropDbMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_USE_DB, mndProcessUseDbMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_SYNC_DB, mndProcessSyncDbMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_COMPACT_DB, mndProcessCompactDbMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_DB, mndGetDbMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveDbs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextDb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDb(SMnode *pMnode) {}

static SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, TSDB_DB_VER_NUMBER, sizeof(SDbObj) + TSDB_DB_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfgVersion)
  SDB_SET_INT32(pRaw, dataPos, pDb->vgVersion)
  SDB_SET_INT8(pRaw, dataPos, pDb->hashMethod)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfVgroups)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheBlockSize)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.totalBlocks)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRows)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRows)
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

  if (sver != TSDB_DB_VER_NUMBER) {
    mError("failed to decode db since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SDbObj));
  SDbObj  *pDb = sdbGetRowObj(pRow);
  if (pDb == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDb->name, TSDB_DB_FNAME_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDb->acct, TSDB_USER_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->updateTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDb->uid)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfgVersion)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->vgVersion)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pDb->hashMethod)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.numOfVgroups)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.cacheBlockSize)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.totalBlocks)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysPerFile)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep0)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep1)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.daysToKeep2)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.minRows)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDb->cfg.maxRows)
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
  pOldDb->updateTime = pNewDb->updateTime;
  pOldDb->cfgVersion = pNewDb->cfgVersion;
  pOldDb->vgVersion = pNewDb->vgVersion;
  memcpy(&pOldDb->cfg, &pNewDb->cfg, sizeof(SDbCfg));
  return 0;
}

SDbObj *mndAcquireDb(SMnode *pMnode, char *db) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = sdbAcquire(pSdb, SDB_DB, db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
  }
  return pDb;
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

static int32_t mndCheckDbCfg(SMnode *pMnode, SDbCfg *pCfg) {
  if (pCfg->numOfVgroups < TSDB_MIN_VNODES_PER_DB || pCfg->numOfVgroups > TSDB_MAX_VNODES_PER_DB) return -1;
  if (pCfg->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCfg->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) return -1;
  if (pCfg->totalBlocks < TSDB_MIN_TOTAL_BLOCKS || pCfg->totalBlocks > TSDB_MAX_TOTAL_BLOCKS) return -1;
  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return -1;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return -1;
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return -1;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return -1;
  if (pCfg->minRows < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRows > TSDB_MAX_MIN_ROW_FBLOCK) return -1;
  if (pCfg->maxRows < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRows > TSDB_MAX_MAX_ROW_FBLOCK) return -1;
  if (pCfg->minRows > pCfg->maxRows) return -1;
  if (pCfg->commitTime < TSDB_MIN_COMMIT_TIME || pCfg->commitTime > TSDB_MAX_COMMIT_TIME) return -1;
  if (pCfg->fsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->fsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return -1;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return -1;
  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) return -1;
  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) return -1;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA_OPTION || pCfg->replications > TSDB_MAX_DB_REPLICA_OPTION) return -1;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) return -1;
  if (pCfg->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCfg->quorum > TSDB_MAX_DB_QUORUM_OPTION) return -1;
  if (pCfg->quorum > pCfg->replications) return -1;
  if (pCfg->update < TSDB_MIN_DB_UPDATE || pCfg->update > TSDB_MAX_DB_UPDATE) return -1;
  if (pCfg->cacheLastRow < TSDB_MIN_DB_CACHE_LAST_ROW || pCfg->cacheLastRow > TSDB_MAX_DB_CACHE_LAST_ROW) return -1;
  return TSDB_CODE_SUCCESS;
}

static void mndSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->numOfVgroups < 0) pCfg->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  if (pCfg->cacheBlockSize < 0) pCfg->cacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
  if (pCfg->totalBlocks < 0) pCfg->totalBlocks = TSDB_DEFAULT_TOTAL_BLOCKS;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  if (pCfg->daysToKeep0 < 0) pCfg->daysToKeep0 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = TSDB_DEFAULT_KEEP;
  if (pCfg->minRows < 0) pCfg->minRows = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  if (pCfg->maxRows < 0) pCfg->maxRows = TSDB_DEFAULT_MAX_ROW_FBLOCK;
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

static int32_t mndSetCreateDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendRedolog(pTrans, pVgRaw) != 0) return -1;
    if (sdbSetRawStatus(pVgRaw, SDB_STATUS_CREATING) != 0) return -1;
  }

  return 0;
}

static int32_t mndSetCreateDbUndoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_DROPPED) != 0) return -1;

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendUndolog(pTrans, pVgRaw) != 0) return -1;
    if (sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED) != 0) return -1;
  }

  return 0;
}

static int32_t mndSetCreateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_READY) != 0) return -1;

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) return -1;
    if (sdbSetRawStatus(pVgRaw, SDB_STATUS_READY) != 0) return -1;
  }

  return 0;
}

static int32_t mndSetCreateDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      STransAction action = {0};
      SVnodeGid   *pVgid = pVgroup->vnodeGid + vn;

      SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
      if (pDnode == NULL) return -1;
      action.epSet = mndGetDnodeEpset(pDnode);
      mndReleaseDnode(pMnode, pDnode);

      SCreateVnodeMsg *pMsg = mndBuildCreateVnodeMsg(pMnode, pDnode, pDb, pVgroup);
      if (pMsg == NULL) return -1;

      action.pCont = pMsg;
      action.contLen = sizeof(SCreateVnodeMsg);
      action.msgType = TDMT_DND_CREATE_VNODE;
      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        free(pMsg);
        return -1;
      }
    }
  }

  return 0;
}

static int32_t mndSetCreateDbUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      STransAction action = {0};
      SVnodeGid   *pVgid = pVgroup->vnodeGid + vn;

      SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
      if (pDnode == NULL) return -1;
      action.epSet = mndGetDnodeEpset(pDnode);
      mndReleaseDnode(pMnode, pDnode);

      SDropVnodeMsg *pMsg = mndBuildDropVnodeMsg(pMnode, pDnode, pDb, pVgroup);
      if (pMsg == NULL) return -1;

      action.pCont = pMsg;
      action.contLen = sizeof(SDropVnodeMsg);
      action.msgType = TDMT_DND_DROP_VNODE;
      if (mndTransAppendUndoAction(pTrans, &action) != 0) {
        free(pMsg);
        return -1;
      }
    }
  }

  return 0;
}

static int32_t mndCreateDb(SMnode *pMnode, SMnodeMsg *pMsg, SCreateDbMsg *pCreate, SUserObj *pUser) {
  SDbObj dbObj = {0};
  memcpy(dbObj.name, pCreate->db, TSDB_DB_FNAME_LEN);
  memcpy(dbObj.acct, pUser->acct, TSDB_USER_LEN);
  dbObj.createdTime = taosGetTimestampMs();
  dbObj.updateTime = dbObj.createdTime;
  dbObj.uid = mndGenerateUid(dbObj.name, TSDB_DB_FNAME_LEN);
  dbObj.cfgVersion = 1;
  dbObj.vgVersion = 1;
  dbObj.hashMethod = 1;
  dbObj.cfg = (SDbCfg){.numOfVgroups = pCreate->numOfVgroups,
                       .cacheBlockSize = pCreate->cacheBlockSize,
                       .totalBlocks = pCreate->totalBlocks,
                       .daysPerFile = pCreate->daysPerFile,
                       .daysToKeep0 = pCreate->daysToKeep0,
                       .daysToKeep1 = pCreate->daysToKeep1,
                       .daysToKeep2 = pCreate->daysToKeep2,
                       .minRows = pCreate->minRows,
                       .maxRows = pCreate->maxRows,
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

  if (mndCheckDbCfg(pMnode, &dbObj.cfg) != 0) {
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  SVgObj *pVgroups = NULL;
  if (mndAllocVgroup(pMnode, &dbObj, &pVgroups) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    goto CREATE_DB_OVER;
  }

  mDebug("trans:%d, used to create db:%s", pTrans->id, pCreate->db);

  if (mndSetCreateDbRedoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  if (mndSetCreateDbUndoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) {
    mError("trans:%d, failed to set undo log since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  if (mndSetCreateDbCommitLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  if (mndSetCreateDbRedoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  if (mndSetCreateDbUndoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto CREATE_DB_OVER;
  }

  code = 0;

CREATE_DB_OVER:
  free(pVgroups);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateDbMsg(SMnodeMsg *pMsg) {
  SMnode       *pMnode = pMsg->pMnode;
  SCreateDbMsg *pCreate = pMsg->rpcMsg.pCont;

  pCreate->numOfVgroups = htonl(pCreate->numOfVgroups);
  pCreate->cacheBlockSize = htonl(pCreate->cacheBlockSize);
  pCreate->totalBlocks = htonl(pCreate->totalBlocks);
  pCreate->daysPerFile = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep0 = htonl(pCreate->daysToKeep0);
  pCreate->daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCreate->minRows = htonl(pCreate->minRows);
  pCreate->maxRows = htonl(pCreate->maxRows);
  pCreate->commitTime = htonl(pCreate->commitTime);
  pCreate->fsyncPeriod = htonl(pCreate->fsyncPeriod);

  mDebug("db:%s, start to create", pCreate->db);

  SDbObj *pDb = mndAcquireDb(pMnode, pCreate->db);
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
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
  terrno = TSDB_CODE_MND_DB_OPTION_UNCHANGED;

  if (pAlter->totalBlocks >= 0 && pAlter->totalBlocks != pDb->cfg.totalBlocks) {
    pDb->cfg.totalBlocks = pAlter->totalBlocks;
    terrno = 0;
  }

  if (pAlter->daysToKeep0 >= 0 && pAlter->daysToKeep0 != pDb->cfg.daysToKeep0) {
    pDb->cfg.daysToKeep0 = pAlter->daysToKeep0;
    terrno = 0;
  }

  if (pAlter->daysToKeep1 >= 0 && pAlter->daysToKeep1 != pDb->cfg.daysToKeep1) {
    pDb->cfg.daysToKeep1 = pAlter->daysToKeep1;
    terrno = 0;
  }

  if (pAlter->daysToKeep2 >= 0 && pAlter->daysToKeep2 != pDb->cfg.daysToKeep2) {
    pDb->cfg.daysToKeep2 = pAlter->daysToKeep2;
    terrno = 0;
  }

  if (pAlter->fsyncPeriod >= 0 && pAlter->fsyncPeriod != pDb->cfg.fsyncPeriod) {
    pDb->cfg.fsyncPeriod = pAlter->fsyncPeriod;
    terrno = 0;
  }

  if (pAlter->walLevel >= 0 && pAlter->walLevel != pDb->cfg.walLevel) {
    pDb->cfg.walLevel = pAlter->walLevel;
    terrno = 0;
  }

  if (pAlter->quorum >= 0 && pAlter->quorum != pDb->cfg.quorum) {
    pDb->cfg.quorum = pAlter->quorum;
    terrno = 0;
  }

  if (pAlter->cacheLastRow >= 0 && pAlter->cacheLastRow != pDb->cfg.cacheLastRow) {
    pDb->cfg.cacheLastRow = pAlter->cacheLastRow;
    terrno = 0;
  }

  return terrno;
}

static int32_t mndSetUpdateDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOldDb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_UPDATING) != 0) return -1;

  return 0;
}

static int32_t mndSetUpdateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb) { 
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNewDb);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndBuildUpdateVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    STransAction action = {0};
    SVnodeGid   *pVgid = pVgroup->vnodeGid + vn;

    SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) return -1;
    action.epSet = mndGetDnodeEpset(pDnode);
    mndReleaseDnode(pMnode, pDnode);

    SAlterVnodeMsg *pMsg = (SAlterVnodeMsg *)mndBuildCreateVnodeMsg(pMnode, pDnode, pDb, pVgroup);
    if (pMsg == NULL) return -1;

    action.pCont = pMsg;
    action.contLen = sizeof(SAlterVnodeMsg);
    action.msgType = TDMT_DND_ALTER_VNODE;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pMsg);
      return -1;
    }
  }

  return 0;
}

static int32_t mndSetUpdateDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pNewDb->uid) {
      if (mndBuildUpdateVgroupAction(pMnode, pTrans, pNewDb, pVgroup) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        return -1;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndUpdateDb(SMnode *pMnode, SMnodeMsg *pMsg, SDbObj *pOldDb, SDbObj *pNewDb) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("db:%s, failed to update since %s", pOldDb->name, terrstr());
    return terrno;
  }

  mDebug("trans:%d, used to update db:%s", pTrans->id, pOldDb->name);

  if (mndSetUpdateDbRedoLogs(pMnode, pTrans, pOldDb, pNewDb) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto UPDATE_DB_OVER;
  }

  if (mndSetUpdateDbCommitLogs(pMnode, pTrans, pOldDb, pNewDb) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto UPDATE_DB_OVER;
  }

  if (mndSetUpdateDbRedoActions(pMnode, pTrans, pOldDb, pNewDb) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto UPDATE_DB_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto UPDATE_DB_OVER;
  }

  code = 0;

UPDATE_DB_OVER:
  mndTransDrop(pTrans);
  return code;
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

  dbObj.cfgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  code = mndUpdateDb(pMnode, pMsg, pDb, &dbObj);
  mndReleaseDb(pMnode, pDb);

  if (code != 0) {
    mError("db:%s, failed to alter since %s", pAlter->db, tstrerror(code));
    return code;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndSetDropDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pDb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;

  return 0;
}

static int32_t mndSetDropDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdbRaw *pCommitRaw = mndDbActionEncode(pDb);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
      if (pVgRaw == NULL || mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        return -1;
      }
      sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED);
    }

    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndBuildDropVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    STransAction action = {0};
    SVnodeGid *  pVgid = pVgroup->vnodeGid + vn;

    SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) return -1;
    action.epSet = mndGetDnodeEpset(pDnode);
    mndReleaseDnode(pMnode, pDnode);

    SDropVnodeMsg *pMsg = mndBuildDropVnodeMsg(pMnode, pDnode, pDb, pVgroup);
    if (pMsg == NULL) return -1;

    action.pCont = pMsg;
    action.contLen = sizeof(SCreateVnodeMsg);
    action.msgType = TDMT_DND_DROP_VNODE;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pMsg);
      return -1;
    }
  }

  return 0;
}

static int32_t mndSetDropDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      if (mndBuildDropVgroupAction(pMnode, pTrans, pDb, pVgroup) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        return -1;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndDropDb(SMnode *pMnode, SMnodeMsg *pMsg, SDbObj *pDb) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("db:%s, failed to drop since %s", pDb->name, terrstr());
    return -1;
  }

  mDebug("trans:%d, used to drop db:%s", pTrans->id, pDb->name);

  if (mndSetDropDbRedoLogs(pMnode, pTrans, pDb) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto DROP_DB_OVER;
  }

  if (mndSetDropDbCommitLogs(pMnode, pTrans, pDb) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto DROP_DB_OVER;
  }

  if (mndSetDropDbRedoActions(pMnode, pTrans, pDb) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto DROP_DB_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto DROP_DB_OVER;
  }

  code = 0;

DROP_DB_OVER:
  mndTransDrop(pTrans);
  return code;
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
  SSdb      *pSdb = pMnode->pSdb;
  SUseDbMsg *pUse = pMsg->rpcMsg.pCont;
  pUse->vgVersion = htonl(pUse->vgVersion);

  SDbObj *pDb = mndAcquireDb(pMnode, pUse->db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    mError("db:%s, failed to process use db msg since %s", pUse->db, terrstr());
    return -1;
  }

  int32_t    contLen = sizeof(SUseDbRsp) + pDb->cfg.numOfVgroups * sizeof(SVgroupInfo);
  SUseDbRsp *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t vindex = 0;

  if (pUse->vgVersion < pDb->vgVersion) {
    void *pIter = NULL;
    while (vindex < pDb->cfg.numOfVgroups) {
      SVgObj *pVgroup = NULL;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;

      if (pVgroup->dbUid == pDb->uid) {
        SVgroupInfo *pInfo = &pRsp->vgroupInfo[vindex];
        pInfo->vgId = htonl(pVgroup->vgId);
        pInfo->hashBegin = htonl(pVgroup->hashBegin);
        pInfo->hashEnd = htonl(pVgroup->hashEnd);
        pInfo->numOfEps = pVgroup->replica;
        for (int32_t gid = 0; gid < pVgroup->replica; ++gid) {
          SVnodeGid  *pVgid = &pVgroup->vnodeGid[gid];
          SEpAddrMsg *pEpArrr = &pInfo->epAddr[gid];
          SDnodeObj  *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
          if (pDnode != NULL) {
            memcpy(pEpArrr->fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
            pEpArrr->port = htons(pDnode->port);
          }
          mndReleaseDnode(pMnode, pDnode);
          if (pVgid->role == TAOS_SYNC_STATE_LEADER) {
            pInfo->inUse = gid;
          }
        }
        vindex++;
      }

      sdbRelease(pSdb, pVgroup);
    }
  }

  memcpy(pRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
  pRsp->vgVersion = htonl(pDb->vgVersion);
  pRsp->vgNum = htonl(vindex);
  pRsp->hashMethod = pDb->hashMethod;

  pMsg->pCont = pRsp;
  pMsg->contLen = contLen;
  mndReleaseDb(pMnode, pDb);

  return 0;
}

static int32_t mndProcessSyncDbMsg(SMnodeMsg *pMsg) {
  SMnode     *pMnode = pMsg->pMnode;
  SSyncDbMsg *pSync = pMsg->rpcMsg.pCont;
  SDbObj     *pDb = mndAcquireDb(pMnode, pSync->db);
  if (pDb == NULL) {
    mError("db:%s, failed to process sync db msg since %s", pSync->db, terrstr());
    return -1;
  }

  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndProcessCompactDbMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SCompactDbMsg *pCompact = pMsg->rpcMsg.pCont;
  SDbObj        *pDb = mndAcquireDb(pMnode, pCompact->db);
  if (pDb == NULL) {
    mError("db:%s, failed to process compact db msg since %s", pCompact->db, terrstr());
    return -1;
  }

  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndGetDbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vgroups");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "ntables");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "replica");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "quorum");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "days");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep0,keep1,keep2");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "cache");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "blocks");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "minrows");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "maxrows");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "wallevel");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "fsync");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "comp");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "cachelast");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 3 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "precision");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "update");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
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
    *(int16_t *)pWrite = pDb->cfg.numOfVgroups;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = 0;  // todo
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
    *(int32_t *)pWrite = pDb->cfg.minRows;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.maxRows;
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