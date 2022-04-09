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
#include "mndAuth.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "mndSma.h"

#define TSDB_DB_VER_NUMBER   1
#define TSDB_DB_RESERVE_SIZE 64

static SSdbRaw *mndDbActionEncode(SDbObj *pDb);
static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw);
static int32_t  mndDbActionInsert(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionDelete(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionUpdate(SSdb *pSdb, SDbObj *pOld, SDbObj *pNew);
static int32_t  mndProcessCreateDbReq(SNodeMsg *pReq);
static int32_t  mndProcessAlterDbReq(SNodeMsg *pReq);
static int32_t  mndProcessDropDbReq(SNodeMsg *pReq);
static int32_t  mndProcessUseDbReq(SNodeMsg *pReq);
static int32_t  mndProcessSyncDbReq(SNodeMsg *pReq);
static int32_t  mndProcessCompactDbReq(SNodeMsg *pReq);
static int32_t  mndGetDbMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveDbs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextDb(SMnode *pMnode, void *pIter);
static int32_t  mndProcessGetDbCfgReq(SNodeMsg *pReq);
static int32_t  mndProcessGetIndexReq(SNodeMsg *pReq);

int32_t mndInitDb(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_DB,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndDbActionEncode,
                     .decodeFp = (SdbDecodeFp)mndDbActionDecode,
                     .insertFp = (SdbInsertFp)mndDbActionInsert,
                     .updateFp = (SdbUpdateFp)mndDbActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndDbActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DB, mndProcessCreateDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_DB, mndProcessAlterDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DB, mndProcessDropDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_USE_DB, mndProcessUseDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SYNC_DB, mndProcessSyncDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMPACT_DB, mndProcessCompactDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_DB_CFG, mndProcessGetDbCfgReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_INDEX, mndProcessGetIndexReq);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_DB, mndGetDbMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveDbs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextDb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDb(SMnode *pMnode) {}

static SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, TSDB_DB_VER_NUMBER,
                              sizeof(SDbObj) + pDb->cfg.numOfRetensions * sizeof(SRetention) + TSDB_DB_RESERVE_SIZE);
  if (pRaw == NULL) goto DB_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, DB_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, DB_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, DB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime, DB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime, DB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfgVersion, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->vgVersion, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->hashMethod, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfVgroups, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheBlockSize, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.totalBlocks, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRows, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRows, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.commitTime, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.fsyncPeriod, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.quorum, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.update, DB_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLastRow, DB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfRetensions, DB_ENCODE_OVER)
  for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pDb->cfg.pRetensions, i);
    SDB_SET_INT32(pRaw, dataPos, pRetension->freq, DB_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pRetension->keep, DB_ENCODE_OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->freqUnit, DB_ENCODE_OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->keepUnit, DB_ENCODE_OVER)
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_DB_RESERVE_SIZE, DB_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, DB_ENCODE_OVER)

  terrno = 0;

DB_ENCODE_OVER:
  if (terrno != 0) {
    mError("db:%s, failed to encode to raw:%p since %s", pDb->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("db:%s, encode to raw:%p, row:%p", pDb->name, pRaw, pDb);
  return pRaw;
}

static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto DB_DECODE_OVER;

  if (sver != TSDB_DB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto DB_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SDbObj));
  if (pRow == NULL) goto DB_DECODE_OVER;

  SDbObj *pDb = sdbGetRowObj(pRow);
  if (pDb == NULL) goto DB_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, DB_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, DB_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, DB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->createdTime, DB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->updateTime, DB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->uid, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfgVersion, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->vgVersion, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->hashMethod, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfVgroups, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.cacheBlockSize, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.totalBlocks, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysPerFile, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep0, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep1, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep2, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.minRows, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.maxRows, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.commitTime, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.fsyncPeriod, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.walLevel, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.precision, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.compression, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.replications, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.quorum, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.update, DB_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.cacheLastRow, DB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfRetensions, DB_DECODE_OVER)
  if (pDb->cfg.numOfRetensions > 0) {
    pDb->cfg.pRetensions = taosArrayInit(pDb->cfg.numOfRetensions, sizeof(SRetention));
    if (pDb->cfg.pRetensions == NULL) goto DB_DECODE_OVER;
    for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
      SRetention retension = {0};
      SDB_GET_INT32(pRaw, dataPos, &retension.freq, DB_DECODE_OVER)
      SDB_GET_INT32(pRaw, dataPos, &retension.keep, DB_DECODE_OVER)
      SDB_GET_INT8(pRaw, dataPos, &retension.freqUnit, DB_DECODE_OVER)
      SDB_GET_INT8(pRaw, dataPos, &retension.keepUnit, DB_DECODE_OVER)
      if (taosArrayPush(pDb->cfg.pRetensions, &retension) == NULL) {
        goto DB_DECODE_OVER;
      }
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_DB_RESERVE_SIZE, DB_DECODE_OVER)

  terrno = 0;

DB_DECODE_OVER:
  if (terrno != 0) {
    mError("db:%s, failed to decode from raw:%p since %s", pDb->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("db:%s, decode from raw:%p, row:%p", pDb->name, pRaw, pDb);
  return pRow;
}

static int32_t mndDbActionInsert(SSdb *pSdb, SDbObj *pDb) {
  mTrace("db:%s, perform insert action, row:%p", pDb->name, pDb);
  return 0;
}

static int32_t mndDbActionDelete(SSdb *pSdb, SDbObj *pDb) {
  mTrace("db:%s, perform delete action, row:%p", pDb->name, pDb);
  taosArrayDestroy(pDb->cfg.pRetensions);
  return 0;
}

static int32_t mndDbActionUpdate(SSdb *pSdb, SDbObj *pOld, SDbObj *pNew) {
  mTrace("db:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  pOld->cfgVersion = pNew->cfgVersion;
  pOld->vgVersion = pNew->vgVersion;
  memcpy(&pOld->cfg, &pNew->cfg, sizeof(SDbCfg));
  return 0;
}

SDbObj *mndAcquireDb(SMnode *pMnode, const char *db) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = sdbAcquire(pSdb, SDB_DB, db);
  if (pDb == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
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
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return -1;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return -1;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return -1;
  if (pCfg->minRows < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRows > TSDB_MAX_MIN_ROW_FBLOCK) return -1;
  if (pCfg->maxRows < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRows > TSDB_MAX_MAX_ROW_FBLOCK) return -1;
  if (pCfg->minRows > pCfg->maxRows) return -1;
  if (pCfg->commitTime < TSDB_MIN_COMMIT_TIME || pCfg->commitTime > TSDB_MAX_COMMIT_TIME) return -1;
  if (pCfg->fsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->fsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return -1;
  if (pCfg->ttl < TSDB_MIN_DB_TTL_OPTION && pCfg->ttl != TSDB_DEFAULT_DB_TTL_OPTION) return -1;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return -1;
  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) return -1;
  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) return -1;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA_OPTION || pCfg->replications > TSDB_MAX_DB_REPLICA_OPTION) return -1;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) return -1;
  if (pCfg->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCfg->quorum > TSDB_MAX_DB_QUORUM_OPTION) return -1;
  if (pCfg->quorum > pCfg->replications) return -1;
  if (pCfg->update < TSDB_MIN_DB_UPDATE || pCfg->update > TSDB_MAX_DB_UPDATE) return -1;
  if (pCfg->cacheLastRow < TSDB_MIN_DB_CACHE_LAST_ROW || pCfg->cacheLastRow > TSDB_MAX_DB_CACHE_LAST_ROW) return -1;
  if (pCfg->streamMode < TSDB_MIN_DB_STREAM_MODE || pCfg->streamMode > TSDB_MAX_DB_STREAM_MODE) return -1;
  if (pCfg->singleSTable < TSDB_MIN_DB_SINGLE_STABLE_OPTION || pCfg->streamMode > TSDB_MAX_DB_SINGLE_STABLE_OPTION) return -1;
  return TSDB_CODE_SUCCESS;
}

static void mndSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->numOfVgroups < 0) pCfg->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  if (pCfg->cacheBlockSize < 0) pCfg->cacheBlockSize = TSDB_DEFAULT_CACHE_BLOCK_SIZE;
  if (pCfg->totalBlocks < 0) pCfg->totalBlocks = TSDB_DEFAULT_TOTAL_BLOCKS;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  if (pCfg->daysToKeep0 < 0) pCfg->daysToKeep0 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = pCfg->daysToKeep0;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = pCfg->daysToKeep1;
  if (pCfg->minRows < 0) pCfg->minRows = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  if (pCfg->maxRows < 0) pCfg->maxRows = TSDB_DEFAULT_MAX_ROW_FBLOCK;
  if (pCfg->commitTime < 0) pCfg->commitTime = TSDB_DEFAULT_COMMIT_TIME;
  if (pCfg->fsyncPeriod < 0) pCfg->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  if (pCfg->ttl < 0) pCfg->ttl = TSDB_DEFAULT_DB_TTL_OPTION;
  if (pCfg->walLevel < 0) pCfg->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  if (pCfg->precision < 0) pCfg->precision = TSDB_DEFAULT_PRECISION;
  if (pCfg->compression < 0) pCfg->compression = TSDB_DEFAULT_COMP_LEVEL;
  if (pCfg->replications < 0) pCfg->replications = TSDB_DEFAULT_DB_REPLICA_OPTION;
  if (pCfg->quorum < 0) pCfg->quorum = TSDB_DEFAULT_DB_QUORUM_OPTION;
  if (pCfg->update < 0) pCfg->update = TSDB_DEFAULT_DB_UPDATE_OPTION;
  if (pCfg->cacheLastRow < 0) pCfg->cacheLastRow = TSDB_DEFAULT_CACHE_LAST_ROW;
  if (pCfg->streamMode < 0) pCfg->streamMode = TSDB_DEFAULT_DB_STREAM_MODE;
  if (pCfg->singleSTable < 0) pCfg->singleSTable = TSDB_DEFAULT_DB_SINGLE_STABLE_OPTION;
  if (pCfg->numOfRetensions < 0) pCfg->numOfRetensions = 0;
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

      int32_t contLen = 0;
      void   *pReq = mndBuildCreateVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
      if (pReq == NULL) return -1;

      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_CREATE_VNODE;
      action.acceptableCode = TSDB_CODE_DND_VNODE_ALREADY_DEPLOYED;
      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        taosMemoryFree(pReq);
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

      int32_t contLen = 0;
      void   *pReq = mndBuildDropVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
      if (pReq == NULL) return -1;

      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_DROP_VNODE;
      action.acceptableCode = TSDB_CODE_DND_VNODE_NOT_DEPLOYED;
      if (mndTransAppendUndoAction(pTrans, &action) != 0) {
        taosMemoryFree(pReq);
        return -1;
      }
    }
  }

  return 0;
}

static int32_t mndCreateDb(SMnode *pMnode, SNodeMsg *pReq, SCreateDbReq *pCreate, SUserObj *pUser) {
  SDbObj dbObj = {0};
  memcpy(dbObj.name, pCreate->db, TSDB_DB_FNAME_LEN);
  memcpy(dbObj.acct, pUser->acct, TSDB_USER_LEN);
  dbObj.createdTime = taosGetTimestampMs();
  dbObj.updateTime = dbObj.createdTime;
  dbObj.uid = mndGenerateUid(dbObj.name, TSDB_DB_FNAME_LEN);
  dbObj.cfgVersion = 1;
  dbObj.vgVersion = 1;
  dbObj.hashMethod = 1;
  memcpy(dbObj.createUser, pUser->user, TSDB_USER_LEN);
  dbObj.cfg = (SDbCfg){
      .numOfVgroups = pCreate->numOfVgroups,
      .cacheBlockSize = pCreate->cacheBlockSize,
      .totalBlocks = pCreate->totalBlocks,
      .daysPerFile = pCreate->daysPerFile,
      .daysToKeep0 = pCreate->daysToKeep0,
      .daysToKeep1 = pCreate->daysToKeep1,
      .daysToKeep2 = pCreate->daysToKeep2,
      .minRows = pCreate->minRows,
      .maxRows = pCreate->maxRows,
      .commitTime = pCreate->commitTime,
      .fsyncPeriod = pCreate->fsyncPeriod,
      .ttl = pCreate->ttl,
      .walLevel = pCreate->walLevel,
      .precision = pCreate->precision,
      .compression = pCreate->compression,
      .replications = pCreate->replications,
      .quorum = pCreate->quorum,
      .update = pCreate->update,
      .cacheLastRow = pCreate->cacheLastRow,
      .streamMode = pCreate->streamMode,
      .singleSTable = pCreate->singleSTable,
  };

  dbObj.cfg.numOfRetensions = pCreate->numOfRetensions;
  dbObj.cfg.pRetensions = pCreate->pRetensions;
  pCreate->pRetensions = NULL;

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
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_DB, &pReq->rpcMsg);
  if (pTrans == NULL) goto CREATE_DB_OVER;

  mDebug("trans:%d, used to create db:%s", pTrans->id, pCreate->db);

  mndTransSetDbInfo(pTrans, &dbObj);
  if (mndSetCreateDbRedoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto CREATE_DB_OVER;
  if (mndSetCreateDbUndoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto CREATE_DB_OVER;
  if (mndSetCreateDbCommitLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto CREATE_DB_OVER;
  if (mndSetCreateDbRedoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto CREATE_DB_OVER;
  if (mndSetCreateDbUndoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto CREATE_DB_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto CREATE_DB_OVER;

  code = 0;

CREATE_DB_OVER:
  taosMemoryFree(pVgroups);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateDbReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SUserObj    *pUser = NULL;
  SCreateDbReq createReq = {0};

  if (tDeserializeSCreateDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_DB_OVER;
  }

  mDebug("db:%s, start to create, vgroups:%d", createReq.db, createReq.numOfVgroups);

  pDb = mndAcquireDb(pMnode, createReq.db);
  if (pDb != NULL) {
    if (createReq.ignoreExist) {
      mDebug("db:%s, already exist, ignore exist is set", createReq.db);
      code = 0;
      goto CREATE_DB_OVER;
    } else {
      terrno = TSDB_CODE_MND_DB_ALREADY_EXIST;
      goto CREATE_DB_OVER;
    }
  } else if (terrno != TSDB_CODE_MND_DB_NOT_EXIST) {
    goto CREATE_DB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto CREATE_DB_OVER;
  }

  if (mndCheckCreateDbAuth(pUser) != 0) {
    goto CREATE_DB_OVER;
  }

  code = mndCreateDb(pMnode, pReq, &createReq, pUser);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_DB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to create since %s", createReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  tFreeSCreateDbReq(&createReq);

  return code;
}

static int32_t mndSetDbCfgFromAlterDbReq(SDbObj *pDb, SAlterDbReq *pAlter) {
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

static int32_t mndSetUpdateDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOld);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_UPDATING) != 0) return -1;

  return 0;
}

static int32_t mndSetUpdateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNew);
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

    int32_t contLen = 0;
    void   *pReq = mndBuildCreateVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
    if (pReq == NULL) return -1;

    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_ALTER_VNODE;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

static int32_t mndSetUpdateDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pNew->uid) {
      if (mndBuildUpdateVgroupAction(pMnode, pTrans, pNew, pVgroup) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        return -1;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndUpdateDb(SMnode *pMnode, SNodeMsg *pReq, SDbObj *pOld, SDbObj *pNew) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_ALTER_DB, &pReq->rpcMsg);
  if (pTrans == NULL) goto UPDATE_DB_OVER;

  mDebug("trans:%d, used to update db:%s", pTrans->id, pOld->name);

  mndTransSetDbInfo(pTrans, pOld);
  if (mndSetUpdateDbRedoLogs(pMnode, pTrans, pOld, pNew) != 0) goto UPDATE_DB_OVER;
  if (mndSetUpdateDbCommitLogs(pMnode, pTrans, pOld, pNew) != 0) goto UPDATE_DB_OVER;
  if (mndSetUpdateDbRedoActions(pMnode, pTrans, pOld, pNew) != 0) goto UPDATE_DB_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto UPDATE_DB_OVER;

  code = 0;

UPDATE_DB_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessAlterDbReq(SNodeMsg *pReq) {
  SMnode     *pMnode = pReq->pNode;
  int32_t     code = -1;
  SDbObj     *pDb = NULL;
  SUserObj   *pUser = NULL;
  SAlterDbReq alterReq = {0};

  if (tDeserializeSAlterDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto ALTER_DB_OVER;
  }

  mDebug("db:%s, start to alter", alterReq.db);

  pDb = mndAcquireDb(pMnode, alterReq.db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    goto ALTER_DB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto ALTER_DB_OVER;
  }

  if (mndCheckAlterDropCompactSyncDbAuth(pUser, pDb) != 0) {
    goto ALTER_DB_OVER;
  }

  SDbObj dbObj = {0};
  memcpy(&dbObj, pDb, sizeof(SDbObj));

  code = mndSetDbCfgFromAlterDbReq(&dbObj, &alterReq);
  if (code != 0) {
    goto ALTER_DB_OVER;
  }

  dbObj.cfgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  code = mndUpdateDb(pMnode, pReq, pDb, &dbObj);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

ALTER_DB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to alter since %s", alterReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessGetDbCfgReq(SNodeMsg *pReq) {
  SMnode     *pMnode = pReq->pNode;
  int32_t     code = -1;
  SDbObj     *pDb = NULL;
  SDbCfgReq   cfgReq = {0};
  SDbCfgRsp   cfgRsp = {0};

  if (tDeserializeSDbCfgReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto GET_DB_CFG_OVER;
  }

  pDb = mndAcquireDb(pMnode, cfgReq.db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    goto GET_DB_CFG_OVER;
  }

  cfgRsp.numOfVgroups   = pDb->cfg.numOfVgroups;
  cfgRsp.cacheBlockSize = pDb->cfg.cacheBlockSize;
  cfgRsp.totalBlocks    = pDb->cfg.totalBlocks;
  cfgRsp.daysPerFile    = pDb->cfg.daysPerFile;
  cfgRsp.daysToKeep0    = pDb->cfg.daysToKeep0;
  cfgRsp.daysToKeep1    = pDb->cfg.daysToKeep1;
  cfgRsp.daysToKeep2    = pDb->cfg.daysToKeep2;
  cfgRsp.minRows        = pDb->cfg.minRows;
  cfgRsp.maxRows        = pDb->cfg.maxRows;
  cfgRsp.commitTime     = pDb->cfg.commitTime;
  cfgRsp.fsyncPeriod    = pDb->cfg.fsyncPeriod;
  cfgRsp.ttl            = pDb->cfg.ttl;
  cfgRsp.walLevel       = pDb->cfg.walLevel;
  cfgRsp.precision      = pDb->cfg.precision;
  cfgRsp.compression    = pDb->cfg.compression;
  cfgRsp.replications   = pDb->cfg.replications;
  cfgRsp.quorum         = pDb->cfg.quorum;
  cfgRsp.update         = pDb->cfg.update;
  cfgRsp.cacheLastRow   = pDb->cfg.cacheLastRow;
  cfgRsp.streamMode     = pDb->cfg.streamMode;
  cfgRsp.singleSTable   = pDb->cfg.singleSTable;

  int32_t contLen = tSerializeSDbCfgRsp(NULL, 0, &cfgRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto GET_DB_CFG_OVER;
  }

  tSerializeSDbCfgRsp(pRsp, contLen, &cfgRsp);

  pReq->pRsp = pRsp;
  pReq->rspLen = contLen;

GET_DB_CFG_OVER:

  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to get cfg since %s", cfgReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);

  return code;
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

  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->dbUid == pDb->uid) {
      SSdbRaw *pStbRaw = mndStbActionEncode(pStb);
      if (pStbRaw == NULL || mndTransAppendCommitlog(pTrans, pStbRaw) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pStbRaw);
        return -1;
      }
      sdbSetRawStatus(pStbRaw, SDB_STATUS_DROPPED);
    }

    sdbRelease(pSdb, pStb);
  }

  return 0;
}

static int32_t mndBuildDropVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    STransAction action = {0};
    SVnodeGid   *pVgid = pVgroup->vnodeGid + vn;

    SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) return -1;
    action.epSet = mndGetDnodeEpset(pDnode);
    mndReleaseDnode(pMnode, pDnode);

    int32_t contLen = 0;
    void   *pReq = mndBuildDropVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
    if (pReq == NULL) return -1;

    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_DROP_VNODE;
    action.acceptableCode = TSDB_CODE_DND_VNODE_NOT_DEPLOYED;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
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

static int32_t mndBuildDropDbRsp(SDbObj *pDb, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc) {
  SDropDbRsp dropRsp = {0};
  if (pDb != NULL) {
    memcpy(dropRsp.db, pDb->name, TSDB_DB_FNAME_LEN);
    dropRsp.uid = pDb->uid;
  }

  int32_t rspLen = tSerializeSDropDbRsp(NULL, 0, &dropRsp);
  void   *pRsp = NULL;
  if (useRpcMalloc) {
    pRsp = rpcMallocCont(rspLen);
  } else {
    pRsp = taosMemoryMalloc(rspLen);
  }

  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSDropDbRsp(pRsp, rspLen, &dropRsp);
  *pRspLen = rspLen;
  *ppRsp = pRsp;
  return 0;
}

static int32_t mndDropDb(SMnode *pMnode, SNodeMsg *pReq, SDbObj *pDb) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_DB, &pReq->rpcMsg);
  if (pTrans == NULL) goto DROP_DB_OVER;

  mDebug("trans:%d, used to drop db:%s", pTrans->id, pDb->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetDropDbRedoLogs(pMnode, pTrans, pDb) != 0) goto DROP_DB_OVER;
  if (mndSetDropDbCommitLogs(pMnode, pTrans, pDb) != 0) goto DROP_DB_OVER;
  if (mndSetDropDbRedoActions(pMnode, pTrans, pDb) != 0) goto DROP_DB_OVER;

  int32_t rspLen = 0;
  void   *pRsp = NULL;
  if (mndBuildDropDbRsp(pDb, &rspLen, &pRsp, false) < 0) goto DROP_DB_OVER;
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  if (mndTransPrepare(pMnode, pTrans) != 0) goto DROP_DB_OVER;

  code = 0;

DROP_DB_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropDbReq(SNodeMsg *pReq) {
  SMnode    *pMnode = pReq->pNode;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  SUserObj  *pUser = NULL;
  SDropDbReq dropReq = {0};

  if (tDeserializeSDropDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_DB_OVER;
  }

  mDebug("db:%s, start to drop", dropReq.db);

  pDb = mndAcquireDb(pMnode, dropReq.db);
  if (pDb == NULL) {
    if (dropReq.ignoreNotExists) {
      code = mndBuildDropDbRsp(pDb, &pReq->rspLen, &pReq->pRsp, true);
      goto DROP_DB_OVER;
    } else {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto DROP_DB_OVER;
    }
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto DROP_DB_OVER;
  }

  if (mndCheckAlterDropCompactSyncDbAuth(pUser, pDb) != 0) {
    goto DROP_DB_OVER;
  }

  code = mndDropDb(pMnode, pReq, pDb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_DB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop since %s", dropReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

void mndGetDBTableNum(SDbObj *pDb, SMnode *pMnode, int32_t *num) {
  int32_t vindex = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (vindex < pDb->cfg.numOfVgroups) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      *num += pVgroup->numOfTables / TSDB_TABLE_NUM_UNIT;

      vindex++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  sdbCancelFetch(pSdb, pIter);
}

static void mndBuildDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList) {
  int32_t vindex = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (true) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (NULL == pDb || pVgroup->dbUid == pDb->uid) {
      SVgroupInfo vgInfo = {0};
      vgInfo.vgId = pVgroup->vgId;
      vgInfo.hashBegin = pVgroup->hashBegin;
      vgInfo.hashEnd = pVgroup->hashEnd;
      vgInfo.numOfTable = pVgroup->numOfTables / TSDB_TABLE_NUM_UNIT;
      vgInfo.epSet.numOfEps = pVgroup->replica;
      for (int32_t gid = 0; gid < pVgroup->replica; ++gid) {
        SVnodeGid *pVgid = &pVgroup->vnodeGid[gid];
        SEp       *pEp = &vgInfo.epSet.eps[gid];
        SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
        if (pDnode != NULL) {
          memcpy(pEp->fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
          pEp->port = pDnode->port;
        }
        mndReleaseDnode(pMnode, pDnode);
        if (pVgid->role == TAOS_SYNC_STATE_LEADER) {
          vgInfo.epSet.inUse = gid;
        }
      }
      vindex++;
      taosArrayPush(pVgList, &vgInfo);
    }

    sdbRelease(pSdb, pVgroup);

    if (pDb && (vindex >= pDb->cfg.numOfVgroups)) {
      break;
    }
  }

  sdbCancelFetch(pSdb, pIter);
}

int32_t mndExtractDbInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq) {
  pRsp->pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
  if (pRsp->pVgroupInfos == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t numOfTable = 0;
  mndGetDBTableNum(pDb, pMnode, &numOfTable);

  if (pReq == NULL || pReq->vgVersion < pDb->vgVersion || pReq->dbId != pDb->uid || numOfTable != pReq->numOfTable) {
    mndBuildDBVgroupInfo(pDb, pMnode, pRsp->pVgroupInfos);
  }

  memcpy(pRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
  pRsp->uid = pDb->uid;
  pRsp->vgVersion = pDb->vgVersion;
  pRsp->vgNum = taosArrayGetSize(pRsp->pVgroupInfos);
  pRsp->hashMethod = pDb->hashMethod;
  return 0;
}

static int32_t mndProcessUseDbReq(SNodeMsg *pReq) {
  SMnode   *pMnode = pReq->pNode;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SUserObj *pUser = NULL;
  SUseDbReq usedbReq = {0};
  SUseDbRsp usedbRsp = {0};

  if (tDeserializeSUseDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &usedbReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto USE_DB_OVER;
  }

  char *p = strchr(usedbReq.db, '.');
  if (p && 0 == strcmp(p + 1, TSDB_INFORMATION_SCHEMA_DB)) {
    memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
    static int32_t vgVersion = 1;
    if (usedbReq.vgVersion < vgVersion) {
      usedbRsp.pVgroupInfos = taosArrayInit(10, sizeof(SVgroupInfo));
      if (usedbRsp.pVgroupInfos == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        goto USE_DB_OVER;
      }

      mndBuildDBVgroupInfo(NULL, pMnode, usedbRsp.pVgroupInfos);
      usedbRsp.vgVersion = vgVersion++;

      if (taosArrayGetSize(usedbRsp.pVgroupInfos) <= 0) {
        terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      }
    } else {
      usedbRsp.vgVersion = usedbReq.vgVersion;
      code = 0;
    }
    usedbRsp.vgNum = taosArrayGetSize(usedbRsp.pVgroupInfos);

    // no jump, need to construct rsp
  } else {
    pDb = mndAcquireDb(pMnode, usedbReq.db);
    if (pDb == NULL) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;

      memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
      usedbRsp.uid = usedbReq.dbId;
      usedbRsp.vgVersion = usedbReq.vgVersion;

      mError("db:%s, failed to process use db req since %s", usedbReq.db, terrstr());
    } else {
      pUser = mndAcquireUser(pMnode, pReq->user);
      if (pUser == NULL) {
        goto USE_DB_OVER;
      }

      if (mndCheckUseDbAuth(pUser, pDb) != 0) {
        goto USE_DB_OVER;
      }

      if (mndExtractDbInfo(pMnode, pDb, &usedbRsp, &usedbReq) < 0) {
        goto USE_DB_OVER;
      }

      code = 0;
    }
  }

  int32_t contLen = tSerializeSUseDbRsp(NULL, 0, &usedbRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto USE_DB_OVER;
  }

  tSerializeSUseDbRsp(pRsp, contLen, &usedbRsp);

  pReq->pRsp = pRsp;
  pReq->rspLen = contLen;

USE_DB_OVER:
  if (code != 0) {
    mError("db:%s, failed to process use db req since %s", usedbReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  tFreeSUsedbRsp(&usedbRsp);

  return code;
}

int32_t mndValidateDbInfo(SMnode *pMnode, SDbVgVersion *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen) {
  SUseDbBatchRsp batchUseRsp = {0};
  batchUseRsp.pArray = taosArrayInit(numOfDbs, sizeof(SUseDbRsp));
  if (batchUseRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfDbs; ++i) {
    SDbVgVersion *pDbVgVersion = &pDbs[i];
    pDbVgVersion->dbId = htobe64(pDbVgVersion->dbId);
    pDbVgVersion->vgVersion = htonl(pDbVgVersion->vgVersion);
    pDbVgVersion->numOfTable = htonl(pDbVgVersion->numOfTable);

    SUseDbRsp usedbRsp = {0};

    SDbObj *pDb = mndAcquireDb(pMnode, pDbVgVersion->dbFName);
    if (pDb == NULL) {
      mDebug("db:%s, no exist", pDbVgVersion->dbFName);
      memcpy(usedbRsp.db, pDbVgVersion->dbFName, TSDB_DB_FNAME_LEN);
      usedbRsp.uid = pDbVgVersion->dbId;
      usedbRsp.vgVersion = -1;
      taosArrayPush(batchUseRsp.pArray, &usedbRsp);
      continue;
    }

    int32_t numOfTable = 0;
    mndGetDBTableNum(pDb, pMnode, &numOfTable);

    if (pDbVgVersion->vgVersion >= pDb->vgVersion && numOfTable == pDbVgVersion->numOfTable) {
      mDebug("db:%s, version & numOfTable not changed", pDbVgVersion->dbFName);
      mndReleaseDb(pMnode, pDb);
      continue;
    }

    usedbRsp.pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
    if (usedbRsp.pVgroupInfos == NULL) {
      mndReleaseDb(pMnode, pDb);
      mError("db:%s, failed to malloc usedb response", pDb->name);
      continue;
    }

    mndBuildDBVgroupInfo(pDb, pMnode, usedbRsp.pVgroupInfos);
    memcpy(usedbRsp.db, pDb->name, TSDB_DB_FNAME_LEN);
    usedbRsp.uid = pDb->uid;
    usedbRsp.vgVersion = pDb->vgVersion;
    usedbRsp.vgNum = (int32_t)taosArrayGetSize(usedbRsp.pVgroupInfos);
    usedbRsp.hashMethod = pDb->hashMethod;

    taosArrayPush(batchUseRsp.pArray, &usedbRsp);
    mndReleaseDb(pMnode, pDb);
  }

  int32_t rspLen = tSerializeSUseDbBatchRsp(NULL, 0, &batchUseRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeSUseDbBatchRsp(&batchUseRsp);
    return -1;
  }
  tSerializeSUseDbBatchRsp(pRsp, rspLen, &batchUseRsp);

  *ppRsp = pRsp;
  *pRspLen = rspLen;

  tFreeSUseDbBatchRsp(&batchUseRsp);
  return 0;
}

static int32_t mndProcessSyncDbReq(SNodeMsg *pReq) {
  SMnode    *pMnode = pReq->pNode;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  SUserObj  *pUser = NULL;
  SSyncDbReq syncReq = {0};

  if (tDeserializeSSyncDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &syncReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto SYNC_DB_OVER;
  }

  mDebug("db:%s, start to sync", syncReq.db);

  pDb = mndAcquireDb(pMnode, syncReq.db);
  if (pDb == NULL) {
    goto SYNC_DB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto SYNC_DB_OVER;
  }

  if (mndCheckAlterDropCompactSyncDbAuth(pUser, pDb) != 0) {
    goto SYNC_DB_OVER;
  }

  // code = mndSyncDb();

SYNC_DB_OVER:
  if (code != 0) {
    mError("db:%s, failed to process sync db req since %s", syncReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessCompactDbReq(SNodeMsg *pReq) {
  SMnode       *pMnode = pReq->pNode;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SUserObj     *pUser = NULL;
  SCompactDbReq compactReq = {0};

  if (tDeserializeSSyncDbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &compactReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto SYNC_DB_OVER;
  }

  mDebug("db:%s, start to sync", compactReq.db);

  pDb = mndAcquireDb(pMnode, compactReq.db);
  if (pDb == NULL) {
    goto SYNC_DB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto SYNC_DB_OVER;
  }

  if (mndCheckAlterDropCompactSyncDbAuth(pUser, pDb) != 0) {
    goto SYNC_DB_OVER;
  }

  // code = mndSyncDb();

SYNC_DB_OVER:
  if (code != 0) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndGetDbMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vgroups");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "ntables");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "replica");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "quorum");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "days");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep0,keep1,keep2");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "cache");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "blocks");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "minrows");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "maxrows");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "wallevel");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "fsync");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "comp");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "cachelast");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 3 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "precision");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  //  pShow->bytes[cols] = 1;
  //  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  //  strcpy(pSchema[cols].name, "update");
  //  pSchema[cols].bytes = pShow->bytes[cols];
  //  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_DB);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

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

static char *getDataPosition(char *pData, SShowObj *pShow, int32_t cols, int32_t rows, int32_t capacityOfRow) {
  return pData + pShow->offset[cols] * capacityOfRow + pShow->bytes[cols] * rows;
}

static void dumpDbInfoToPayload(char *data, SDbObj *pDb, SShowObj *pShow, int32_t rows, int32_t rowCapacity,
                                int64_t numOfTables) {
  int32_t cols = 0;

  char *pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  char *name = mnGetDbStr(pDb->name);
  if (name != NULL) {
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, pShow->bytes[cols]);
  } else {
    STR_TO_VARSTR(pWrite, "NULL");
  }
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int64_t *)pWrite = pDb->createdTime;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int16_t *)pWrite = pDb->cfg.numOfVgroups;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int64_t *)pWrite = numOfTables;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int16_t *)pWrite = pDb->cfg.replications;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int16_t *)pWrite = pDb->cfg.quorum;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.daysPerFile;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  char tmp[128] = {0};
  if (pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep1 || pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep2) {
    sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep0);
  } else {
    sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep0, pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2);
  }
  STR_WITH_SIZE_TO_VARSTR(pWrite, tmp, strlen(tmp));
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.cacheBlockSize;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.totalBlocks;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.minRows;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.maxRows;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int8_t *)pWrite = pDb->cfg.walLevel;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.fsyncPeriod;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int8_t *)pWrite = pDb->cfg.compression;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int8_t *)pWrite = pDb->cfg.cacheLastRow;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
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
      prec = "none";
      break;
  }
  STR_WITH_SIZE_TO_VARSTR(pWrite, prec, 2);
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int32_t *)pWrite = pDb->cfg.ttl;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int8_t *)pWrite = pDb->cfg.singleSTable;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  *(int8_t *)pWrite = pDb->cfg.streamMode;
  cols++;

  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  char *status = "ready";
  STR_WITH_SIZE_TO_VARSTR(pWrite, status, strlen(status));
  cols++;

  //  pWrite = getDataPosition(data, pShow, cols, rows, rowCapacity);
  //  *(int8_t *)pWrite = pDb->cfg.update;
}

static void setInformationSchemaDbCfg(SDbObj *pDbObj) {
  ASSERT(pDbObj != NULL);
  strncpy(pDbObj->name, TSDB_INFORMATION_SCHEMA_DB, tListLen(pDbObj->name));

  pDbObj->createdTime = 0;
  pDbObj->cfg.numOfVgroups = 0;
  pDbObj->cfg.quorum = 1;
  pDbObj->cfg.replications = 1;
  pDbObj->cfg.update = 1;
  pDbObj->cfg.precision = TSDB_TIME_PRECISION_MILLI;
}

static bool mndGetTablesOfDbFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int32_t *numOfTables = p1;

  *numOfTables += pVgroup->numOfTables;
  return true;
}

static int32_t mndRetrieveDbs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rowsCapacity) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SDbObj *pDb = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_DB, pShow->pIter, (void **)&pDb);
    if (pShow->pIter == NULL) {
      break;
    }

    int32_t numOfTables = 0;
    sdbTraverse(pSdb, SDB_VGROUP, mndGetTablesOfDbFp, &numOfTables, NULL, NULL);

    dumpDbInfoToPayload(data, pDb, pShow, numOfRows, rowsCapacity, numOfTables);
    numOfRows++;
    sdbRelease(pSdb, pDb);
  }

  // Append the information_schema database into the result.
  if (numOfRows < rowsCapacity) {
    SDbObj dummyISDb = {0};
    setInformationSchemaDbCfg(&dummyISDb);
    dumpDbInfoToPayload(data, &dummyISDb, pShow, numOfRows, rowsCapacity, 14);
    numOfRows += 1;
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rowsCapacity, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextDb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndProcessGetIndexReq(SNodeMsg *pReq) {
  SUserIndexReq indexReq = {0};
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SUserIndexRsp rsp = {0};
  bool exist = false;

  if (tDeserializeSUserIndexReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &indexReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  code = mndProcessGetSmaReq(pMnode, &indexReq, &rsp, &exist);
  if (code) {
    goto _OVER;
  }

  if (!exist) {
    //TODO GET INDEX FROM FULLTEXT
    code = -1;
    terrno = TSDB_CODE_MND_DB_INDEX_NOT_EXIST;
  } else {
    int32_t contLen = tSerializeSUserIndexRsp(NULL, 0, &rsp);
    void   *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      code = -1;
      goto _OVER;
    }
    
    tSerializeSUserIndexRsp(pRsp, contLen, &rsp);
    
    pReq->pRsp = pRsp;
    pReq->rspLen = contLen;

    code = 0;
  }

_OVER:
  if (code != 0) {
    mError("failed to get index %s since %s", indexReq.indexFName, terrstr());
  }

  return code;
}

