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
#include "mndCluster.h"
#include "mndDnode.h"
#include "mndIndex.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "mndView.h"
#include "systable.h"
#include "tjson.h"
#include "thttp.h"
#include "audit.h"

#define DB_VER_NUMBER   1
#define DB_RESERVE_SIZE 42

static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw);
static int32_t  mndDbActionInsert(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionDelete(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionUpdate(SSdb *pSdb, SDbObj *pOld, SDbObj *pNew);
static int32_t  mndNewDbActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw);

static int32_t  mndProcessCreateDbReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterDbReq(SRpcMsg *pReq);
static int32_t  mndProcessDropDbReq(SRpcMsg *pReq);
static int32_t  mndProcessUseDbReq(SRpcMsg *pReq);
static int32_t  mndProcessTrimDbReq(SRpcMsg *pReq);
static int32_t  mndRetrieveDbs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity);
static void     mndCancelGetNextDb(SMnode *pMnode, void *pIter);
static int32_t  mndProcessGetDbCfgReq(SRpcMsg *pReq);

#ifndef TD_ENTERPRISE
int32_t mndProcessCompactDbReq(SRpcMsg *pReq) { return TSDB_CODE_OPS_NOT_SUPPORT; }
#endif

int32_t mndInitDb(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_DB,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndDbActionEncode,
      .decodeFp = (SdbDecodeFp)mndDbActionDecode,
      .insertFp = (SdbInsertFp)mndDbActionInsert,
      .updateFp = (SdbUpdateFp)mndDbActionUpdate,
      .deleteFp = (SdbDeleteFp)mndDbActionDelete,
      .validateFp = (SdbValidateFp)mndNewDbActionValidate,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DB, mndProcessCreateDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_DB, mndProcessAlterDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DB, mndProcessDropDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_USE_DB, mndProcessUseDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMPACT_DB, mndProcessCompactDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TRIM_DB, mndProcessTrimDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_DB_CFG, mndProcessGetDbCfgReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveDbs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextDb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDb(SMnode *pMnode) {}

SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SDbObj) + pDb->cfg.numOfRetensions * sizeof(SRetention) + DB_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, DB_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfgVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->vgVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfVgroups, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfStables, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.buffer, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.pageSize, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.pages, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheLastSize, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRows, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRows, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walFsyncPeriod, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.strict, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLast, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.hashMethod, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfRetensions, _OVER)
  for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pDb->cfg.pRetensions, i);
    SDB_SET_INT64(pRaw, dataPos, pRetension->freq, _OVER)
    SDB_SET_INT64(pRaw, dataPos, pRetension->keep, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->freqUnit, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->keepUnit, _OVER)
  }
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.schemaless, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walRetentionPeriod, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->cfg.walRetentionSize, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walRollPeriod, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->cfg.walSegmentSize, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.sstTrigger, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.hashPrefix, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.hashSuffix, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.tsdbPageSize, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDb->compactStartTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.keepTimeOffset, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, DB_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
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
  SSdbRow *pRow = NULL;
  SDbObj  *pDb = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != DB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SDbObj));
  if (pRow == NULL) goto _OVER;

  pDb = sdbGetRowObj(pRow);
  if (pDb == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->updateTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->uid, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfgVersion, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->vgVersion, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfVgroups, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfStables, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.buffer, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.pageSize, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.pages, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.cacheLastSize, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysPerFile, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep0, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep1, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep2, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.minRows, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.maxRows, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walFsyncPeriod, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.walLevel, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.precision, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.compression, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.replications, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.strict, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.cacheLast, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.hashMethod, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfRetensions, _OVER)
  if (pDb->cfg.numOfRetensions > 0) {
    pDb->cfg.pRetensions = taosArrayInit(pDb->cfg.numOfRetensions, sizeof(SRetention));
    if (pDb->cfg.pRetensions == NULL) goto _OVER;
    for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
      SRetention retention = {0};
      SDB_GET_INT64(pRaw, dataPos, &retention.freq, _OVER)
      SDB_GET_INT64(pRaw, dataPos, &retention.keep, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &retention.freqUnit, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &retention.keepUnit, _OVER)
      if (taosArrayPush(pDb->cfg.pRetensions, &retention) == NULL) {
        goto _OVER;
      }
    }
  }
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.schemaless, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walRetentionPeriod, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->cfg.walRetentionSize, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walRollPeriod, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->cfg.walSegmentSize, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.sstTrigger, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.hashPrefix, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.hashSuffix, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.tsdbPageSize, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDb->compactStartTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.keepTimeOffset, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, DB_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pDb->lock);

  if (pDb->cfg.tsdbPageSize != TSDB_MIN_TSDB_PAGESIZE) {
    mInfo("db:%s, tsdbPageSize set from %d to default %d", pDb->name, pDb->cfg.tsdbPageSize,
          TSDB_DEFAULT_TSDB_PAGESIZE);
  }

  if (pDb->cfg.sstTrigger != TSDB_MIN_STT_TRIGGER) {
    mInfo("db:%s, sstTrigger set from %d to default %d", pDb->name, pDb->cfg.sstTrigger, TSDB_DEFAULT_SST_TRIGGER);
  }

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("db:%s, failed to decode from raw:%p since %s", pDb == NULL ? "null" : pDb->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("db:%s, decode from raw:%p, row:%p", pDb->name, pRaw, pDb);
  return pRow;
}

static int32_t mndNewDbActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw) {
  SSdb    *pSdb = pMnode->pSdb;
  SSdbRow *pRow = NULL;
  SDbObj  *pNewDb = NULL;
  int      code = -1;

  pRow = mndDbActionDecode(pRaw);
  if (pRow == NULL) goto _OVER;
  pNewDb = sdbGetRowObj(pRow);
  if (pNewDb == NULL) goto _OVER;

  SDbObj *pOldDb = sdbAcquire(pMnode->pSdb, SDB_DB, pNewDb->name);
  if (pOldDb != NULL) {
    mError("trans:%d, db name already in use. name: %s", pTrans->id, pNewDb->name);
    sdbRelease(pMnode->pSdb, pOldDb);
    goto _OVER;
  }

  code = 0;
_OVER:
  if (pNewDb) mndDbActionDelete(pSdb, pNewDb);
  taosMemoryFreeClear(pRow);
  return code;
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
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->cfgVersion = pNew->cfgVersion;
  pOld->vgVersion = pNew->vgVersion;
  pOld->cfg.numOfVgroups = pNew->cfg.numOfVgroups;
  pOld->cfg.buffer = pNew->cfg.buffer;
  pOld->cfg.pageSize = pNew->cfg.pageSize;
  pOld->cfg.pages = pNew->cfg.pages;
  pOld->cfg.cacheLastSize = pNew->cfg.cacheLastSize;
  pOld->cfg.daysPerFile = pNew->cfg.daysPerFile;
  pOld->cfg.daysToKeep0 = pNew->cfg.daysToKeep0;
  pOld->cfg.daysToKeep1 = pNew->cfg.daysToKeep1;
  pOld->cfg.daysToKeep2 = pNew->cfg.daysToKeep2;
  pOld->cfg.keepTimeOffset = pNew->cfg.keepTimeOffset;
  pOld->cfg.walFsyncPeriod = pNew->cfg.walFsyncPeriod;
  pOld->cfg.walLevel = pNew->cfg.walLevel;
  pOld->cfg.walRetentionPeriod = pNew->cfg.walRetentionPeriod;
  pOld->cfg.walRetentionSize = pNew->cfg.walRetentionSize;
  pOld->cfg.strict = pNew->cfg.strict;
  pOld->cfg.cacheLast = pNew->cfg.cacheLast;
  pOld->cfg.replications = pNew->cfg.replications;
  pOld->cfg.sstTrigger = pNew->cfg.sstTrigger;
  pOld->cfg.minRows = pNew->cfg.minRows;
  pOld->cfg.maxRows = pNew->cfg.maxRows;
  pOld->cfg.tsdbPageSize = pNew->cfg.tsdbPageSize;
  pOld->compactStartTime = pNew->compactStartTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

static inline int32_t mndGetGlobalVgroupVersion(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetTableVer(pSdb, SDB_VGROUP);
}

SDbObj *mndAcquireDb(SMnode *pMnode, const char *db) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = sdbAcquire(pSdb, SDB_DB, db);
  if (pDb == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_DB_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_DB_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("db:%s, failed to acquire db since %s", db, terrstr());
    }
  }
  return pDb;
}

void mndReleaseDb(SMnode *pMnode, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDb);
}

static int32_t mndCheckDbName(const char *dbName, SUserObj *pUser) {
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
  terrno = TSDB_CODE_MND_INVALID_DB_OPTION;

  if (pCfg->numOfVgroups < TSDB_MIN_VNODES_PER_DB || pCfg->numOfVgroups > TSDB_MAX_VNODES_PER_DB) return -1;
  if (pCfg->numOfStables < TSDB_DB_STREAM_MODE_OFF || pCfg->numOfStables > TSDB_DB_STREAM_MODE_ON) return -1;
  if (pCfg->buffer < TSDB_MIN_BUFFER_PER_VNODE || pCfg->buffer > TSDB_MAX_BUFFER_PER_VNODE) return -1;
  if (pCfg->pageSize < TSDB_MIN_PAGESIZE_PER_VNODE || pCfg->pageSize > TSDB_MAX_PAGESIZE_PER_VNODE) return -1;
  if (pCfg->pages < TSDB_MIN_PAGES_PER_VNODE || pCfg->pages > TSDB_MAX_PAGES_PER_VNODE) return -1;
  if (pCfg->cacheLastSize < TSDB_MIN_DB_CACHE_SIZE || pCfg->cacheLastSize > TSDB_MAX_DB_CACHE_SIZE) return -1;
  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return -1;
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return -1;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return -1;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return -1;
  if (pCfg->keepTimeOffset < TSDB_MIN_KEEP_TIME_OFFSET || pCfg->keepTimeOffset > TSDB_MAX_KEEP_TIME_OFFSET) return -1;
  if (pCfg->minRows < TSDB_MIN_MINROWS_FBLOCK || pCfg->minRows > TSDB_MAX_MINROWS_FBLOCK) return -1;
  if (pCfg->maxRows < TSDB_MIN_MAXROWS_FBLOCK || pCfg->maxRows > TSDB_MAX_MAXROWS_FBLOCK) return -1;
  if (pCfg->minRows > pCfg->maxRows) return -1;
  if (pCfg->walFsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->walFsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return -1;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return -1;
  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) return -1;
  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) return -1;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA || pCfg->replications > TSDB_MAX_DB_REPLICA) return -1;
  if (pCfg->replications != 1 && pCfg->replications != 3) return -1;
  if (pCfg->strict < TSDB_DB_STRICT_OFF || pCfg->strict > TSDB_DB_STRICT_ON) return -1;
  if (pCfg->schemaless < TSDB_DB_SCHEMALESS_OFF || pCfg->schemaless > TSDB_DB_SCHEMALESS_ON) return -1;
  if (pCfg->cacheLast < TSDB_CACHE_MODEL_NONE || pCfg->cacheLast > TSDB_CACHE_MODEL_BOTH) return -1;
  if (pCfg->hashMethod != 1) return -1;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) {
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return -1;
  }
  if (pCfg->walRetentionPeriod < TSDB_DB_MIN_WAL_RETENTION_PERIOD) return -1;
  if (pCfg->walRetentionSize < TSDB_DB_MIN_WAL_RETENTION_SIZE) return -1;
  if (pCfg->walRollPeriod < TSDB_DB_MIN_WAL_ROLL_PERIOD) return -1;
  if (pCfg->walSegmentSize < TSDB_DB_MIN_WAL_SEGMENT_SIZE) return -1;
  if (pCfg->sstTrigger < TSDB_MIN_STT_TRIGGER || pCfg->sstTrigger > TSDB_MAX_STT_TRIGGER) return -1;
  if (pCfg->hashPrefix < TSDB_MIN_HASH_PREFIX || pCfg->hashPrefix > TSDB_MAX_HASH_PREFIX) return -1;
  if (pCfg->hashSuffix < TSDB_MIN_HASH_SUFFIX || pCfg->hashSuffix > TSDB_MAX_HASH_SUFFIX) return -1;
  if ((pCfg->hashSuffix * pCfg->hashPrefix) < 0) return -1;
  if ((pCfg->hashPrefix + pCfg->hashSuffix) >= (TSDB_TABLE_NAME_LEN - 1)) return -1;
  if (pCfg->tsdbPageSize < TSDB_MIN_TSDB_PAGESIZE || pCfg->tsdbPageSize > TSDB_MAX_TSDB_PAGESIZE) return -1;
  if (taosArrayGetSize(pCfg->pRetensions) != pCfg->numOfRetensions) return -1;

  terrno = 0;
  return terrno;
}

static int32_t mndCheckInChangeDbCfg(SMnode *pMnode, SDbCfg *pCfg) {
  terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  if (pCfg->buffer < TSDB_MIN_BUFFER_PER_VNODE || pCfg->buffer > TSDB_MAX_BUFFER_PER_VNODE) return -1;
  if (pCfg->pages < TSDB_MIN_PAGES_PER_VNODE || pCfg->pages > TSDB_MAX_PAGES_PER_VNODE) return -1;
  if (pCfg->pageSize < TSDB_MIN_PAGESIZE_PER_VNODE || pCfg->pageSize > TSDB_MAX_PAGESIZE_PER_VNODE) return -1;
  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return -1;
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return -1;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return -1;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return -1;
  if (pCfg->keepTimeOffset < TSDB_MIN_KEEP_TIME_OFFSET || pCfg->keepTimeOffset > TSDB_MAX_KEEP_TIME_OFFSET) return -1;
  if (pCfg->walFsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->walFsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return -1;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return -1;
  if (pCfg->cacheLast < TSDB_CACHE_MODEL_NONE || pCfg->cacheLast > TSDB_CACHE_MODEL_BOTH) return -1;
  if (pCfg->cacheLastSize < TSDB_MIN_DB_CACHE_SIZE || pCfg->cacheLastSize > TSDB_MAX_DB_CACHE_SIZE) return -1;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA || pCfg->replications > TSDB_MAX_DB_REPLICA) return -1;
  if (pCfg->replications != 1 && pCfg->replications != 3) return -1;
  if (pCfg->sstTrigger < TSDB_MIN_STT_TRIGGER || pCfg->sstTrigger > TSDB_MAX_STT_TRIGGER) return -1;
  if (pCfg->minRows < TSDB_MIN_MINROWS_FBLOCK || pCfg->minRows > TSDB_MAX_MINROWS_FBLOCK) return -1;
  if (pCfg->maxRows < TSDB_MIN_MAXROWS_FBLOCK || pCfg->maxRows > TSDB_MAX_MAXROWS_FBLOCK) return -1;
  if (pCfg->minRows > pCfg->maxRows) return -1;
  if (pCfg->walRetentionPeriod < TSDB_DB_MIN_WAL_RETENTION_PERIOD) return -1;
  if (pCfg->walRetentionSize < TSDB_DB_MIN_WAL_RETENTION_SIZE) return -1;
  if (pCfg->strict < TSDB_DB_STRICT_OFF || pCfg->strict > TSDB_DB_STRICT_ON) return -1;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) {
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return -1;
  }

  terrno = 0;
  return terrno;
}

static void mndSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->numOfVgroups < 0) pCfg->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  if (pCfg->numOfStables < 0) pCfg->numOfStables = TSDB_DEFAULT_DB_SINGLE_STABLE;
  if (pCfg->buffer < 0) pCfg->buffer = TSDB_DEFAULT_BUFFER_PER_VNODE;
  if (pCfg->pageSize < 0) pCfg->pageSize = TSDB_DEFAULT_PAGESIZE_PER_VNODE;
  if (pCfg->pages < 0) pCfg->pages = TSDB_DEFAULT_PAGES_PER_VNODE;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = TSDB_DEFAULT_DURATION_PER_FILE;
  if (pCfg->daysToKeep0 < 0) pCfg->daysToKeep0 = TSDB_DEFAULT_KEEP;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = pCfg->daysToKeep0;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = pCfg->daysToKeep1;
  if (pCfg->keepTimeOffset < 0) pCfg->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (pCfg->minRows < 0) pCfg->minRows = TSDB_DEFAULT_MINROWS_FBLOCK;
  if (pCfg->maxRows < 0) pCfg->maxRows = TSDB_DEFAULT_MAXROWS_FBLOCK;
  if (pCfg->walFsyncPeriod < 0) pCfg->walFsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  if (pCfg->walLevel < 0) pCfg->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  if (pCfg->precision < 0) pCfg->precision = TSDB_DEFAULT_PRECISION;
  if (pCfg->compression < 0) pCfg->compression = TSDB_DEFAULT_COMP_LEVEL;
  if (pCfg->replications < 0) pCfg->replications = TSDB_DEFAULT_DB_REPLICA;
  if (pCfg->strict < 0) pCfg->strict = TSDB_DEFAULT_DB_STRICT;
  if (pCfg->cacheLast < 0) pCfg->cacheLast = TSDB_DEFAULT_CACHE_MODEL;
  if (pCfg->cacheLastSize <= 0) pCfg->cacheLastSize = TSDB_DEFAULT_CACHE_SIZE;
  if (pCfg->numOfRetensions < 0) pCfg->numOfRetensions = 0;
  if (pCfg->schemaless < 0) pCfg->schemaless = TSDB_DB_SCHEMALESS_OFF;
  if (pCfg->walRetentionPeriod < 0 && pCfg->walRetentionPeriod != -1)
    pCfg->walRetentionPeriod = TSDB_REPS_DEF_DB_WAL_RET_PERIOD;
  if (pCfg->walRetentionSize < 0 && pCfg->walRetentionSize != -1)
    pCfg->walRetentionSize = TSDB_REPS_DEF_DB_WAL_RET_SIZE;
  if (pCfg->walRollPeriod < 0) pCfg->walRollPeriod = TSDB_REPS_DEF_DB_WAL_ROLL_PERIOD;
  if (pCfg->walSegmentSize < 0) pCfg->walSegmentSize = TSDB_DEFAULT_DB_WAL_SEGMENT_SIZE;
  if (pCfg->sstTrigger <= 0) pCfg->sstTrigger = TSDB_DEFAULT_SST_TRIGGER;
  if (pCfg->tsdbPageSize <= 0) pCfg->tsdbPageSize = TSDB_DEFAULT_TSDB_PAGESIZE;
}

static int32_t mndSetCreateDbPrepareAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetNewVgPrepareActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    if (mndAddNewVgPrepareAction(pMnode, pTrans, (pVgroups + v)) != 0) return -1;
  }
  return 0;
}

static int32_t mndSetCreateDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_UPDATE) != 0) return -1;

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendRedolog(pTrans, pVgRaw) != 0) return -1;
    if (sdbSetRawStatus(pVgRaw, SDB_STATUS_UPDATE) != 0) return -1;
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

static int32_t mndSetCreateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups,
                                        SUserObj *pUserDuped) {
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

  if (pUserDuped) {
    SSdbRaw *pUserRaw = mndUserActionEncode(pUserDuped);
    if (pUserRaw == NULL) return -1;
    if (mndTransAppendCommitlog(pTrans, pUserRaw) != 0) return -1;
    if (sdbSetRawStatus(pUserRaw, SDB_STATUS_READY) != 0) return -1;
  }

  return 0;
}

static int32_t mndSetCreateDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid) != 0) {
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
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, false) != 0) {
        return -1;
      }
    }
  }

  return 0;
}

static int32_t mndCreateDb(SMnode *pMnode, SRpcMsg *pReq, SCreateDbReq *pCreate, SUserObj *pUser) {
  SDbObj dbObj = {0};
  memcpy(dbObj.name, pCreate->db, TSDB_DB_FNAME_LEN);
  memcpy(dbObj.acct, pUser->acct, TSDB_USER_LEN);
  dbObj.createdTime = taosGetTimestampMs();
  dbObj.updateTime = dbObj.createdTime;
  dbObj.uid = mndGenerateUid(dbObj.name, TSDB_DB_FNAME_LEN);
  dbObj.cfgVersion = 1;
  dbObj.vgVersion = 1;
  memcpy(dbObj.createUser, pUser->user, TSDB_USER_LEN);
  dbObj.cfg = (SDbCfg){
      .numOfVgroups = pCreate->numOfVgroups,
      .numOfStables = pCreate->numOfStables,
      .buffer = pCreate->buffer,
      .pageSize = pCreate->pageSize,
      .pages = pCreate->pages,
      .cacheLastSize = pCreate->cacheLastSize,
      .daysPerFile = pCreate->daysPerFile,
      .daysToKeep0 = pCreate->daysToKeep0,
      .daysToKeep1 = pCreate->daysToKeep1,
      .daysToKeep2 = pCreate->daysToKeep2,
      .keepTimeOffset = pCreate->keepTimeOffset,
      .minRows = pCreate->minRows,
      .maxRows = pCreate->maxRows,
      .walFsyncPeriod = pCreate->walFsyncPeriod,
      .walLevel = pCreate->walLevel,
      .precision = pCreate->precision,
      .compression = pCreate->compression,
      .replications = pCreate->replications,
      .strict = pCreate->strict,
      .cacheLast = pCreate->cacheLast,
      .hashMethod = 1,
      .schemaless = pCreate->schemaless,
      .walRetentionPeriod = pCreate->walRetentionPeriod,
      .walRetentionSize = pCreate->walRetentionSize,
      .walRollPeriod = pCreate->walRollPeriod,
      .walSegmentSize = pCreate->walSegmentSize,
      .sstTrigger = pCreate->sstTrigger,
      .hashPrefix = pCreate->hashPrefix,
      .hashSuffix = pCreate->hashSuffix,
      .tsdbPageSize = pCreate->tsdbPageSize,
  };

  dbObj.cfg.numOfRetensions = pCreate->numOfRetensions;
  dbObj.cfg.pRetensions = pCreate->pRetensions;

  mndSetDefaultDbCfg(&dbObj.cfg);

  if (mndCheckDbName(dbObj.name, pUser) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  if (mndCheckDbCfg(pMnode, &dbObj.cfg) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  if (dbObj.cfg.hashPrefix > 0) {
    int32_t dbLen = strlen(dbObj.name) + 1;
    mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix + dbLen);
    dbObj.cfg.hashPrefix += dbLen;
  } else if (dbObj.cfg.hashPrefix < 0) {
    int32_t dbLen = strlen(dbObj.name) + 1;
    mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix - dbLen);
    dbObj.cfg.hashPrefix -= dbLen;
  }

  SVgObj *pVgroups = NULL;
  if (mndAllocVgroup(pMnode, &dbObj, &pVgroups) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  // add database privileges for user
  SUserObj newUserObj = {0}, *pNewUserDuped = NULL;
  if (!pUser->superUser) {
    if (mndUserDupObj(pUser, &newUserObj) != 0) goto _OVER;
    taosHashPut(newUserObj.readDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name, TSDB_FILENAME_LEN);
    taosHashPut(newUserObj.writeDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name, TSDB_FILENAME_LEN);
    pNewUserDuped = &newUserObj;
  }

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "create-db");
  if (pTrans == NULL) goto _OVER;
  // mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to create db:%s", pTrans->id, pCreate->db);

  mndTransSetDbName(pTrans, dbObj.name, NULL);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  mndTransSetOper(pTrans, MND_OPER_CREATE_DB);
  if (mndSetCreateDbPrepareAction(pMnode, pTrans, &dbObj) != 0) goto _OVER;
  if (mndSetCreateDbRedoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetNewVgPrepareActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbUndoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbCommitLogs(pMnode, pTrans, &dbObj, pVgroups, pNewUserDuped) != 0) goto _OVER;
  if (mndSetCreateDbUndoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  taosMemoryFree(pVgroups);
  mndUserFreeObj(&newUserObj);
  mndTransDrop(pTrans);
  return code;
}

static void mndBuildAuditDetailInt32(char* detail, char* tmp, char* format, int32_t para){
  if(para > 0){
    if(strlen(detail) > 0) strcat(detail, ", ");
    sprintf(tmp, format, para);
    strcat(detail, tmp);
  }
}

static void mndBuildAuditDetailInt64(char* detail, char* tmp, char* format, int64_t para){
  if(para > 0){
    if(strlen(detail) > 0) strcat(detail, ", ");
    sprintf(tmp, format, para);
    strcat(detail, tmp);
  }
}

static int32_t mndProcessCreateDbReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SUserObj    *pUser = NULL;
  SCreateDbReq createReq = {0};

  if ((terrno = grantCheck(TSDB_GRANT_DB)) != 0) {
    code = terrno;
    goto _OVER;
  }

  if (tDeserializeSCreateDbReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }
#ifdef WINDOWS
  if (taosArrayGetSize(createReq.pRetensions) > 0) {
    terrno = TSDB_CODE_MND_INVALID_PLATFORM;
    goto _OVER;
  }
#endif
  mInfo("db:%s, start to create, vgroups:%d", createReq.db, createReq.numOfVgroups);
  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DB, NULL) != 0) {
    goto _OVER;
  }

  pDb = mndAcquireDb(pMnode, createReq.db);
  if (pDb != NULL) {
    if (createReq.ignoreExist) {
      mInfo("db:%s, already exist, ignore exist is set", createReq.db);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_DB_ALREADY_EXIST;
      goto _OVER;
    }
  } else {
    if (terrno == TSDB_CODE_MND_DB_IN_CREATING) {
      code = terrno;
      goto _OVER;
    } else if (terrno == TSDB_CODE_MND_DB_IN_DROPPING) {
      goto _OVER;
    } else if (terrno == TSDB_CODE_MND_DB_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_APP_ERROR
      goto _OVER;
    }
  }

  pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  code = mndCreateDb(pMnode, pReq, &createReq, pUser);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName name = {0};
  tNameFromString(&name, createReq.db, T_NAME_ACCT | T_NAME_DB);

  auditRecord(pReq, pMnode->clusterId, "createDB", name.dbname, "", createReq.sql, createReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to create since %s", createReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  tFreeSCreateDbReq(&createReq);

  return code;
}

static int32_t mndSetDbCfgFromAlterDbReq(SDbObj *pDb, SAlterDbReq *pAlter) {
  terrno = TSDB_CODE_MND_DB_OPTION_UNCHANGED;

  if (pAlter->buffer > 0 && pAlter->buffer != pDb->cfg.buffer) {
    pDb->cfg.buffer = pAlter->buffer;
    terrno = 0;
  }

  if (pAlter->pages > 0 && pAlter->pages != pDb->cfg.pages) {
    pDb->cfg.pages = pAlter->pages;
    terrno = 0;
  }

  if (pAlter->pageSize > 0 && pAlter->pageSize != pDb->cfg.pageSize) {
    pDb->cfg.pageSize = pAlter->pageSize;
    terrno = 0;
  }

  if (pAlter->daysPerFile > 0 && pAlter->daysPerFile != pDb->cfg.daysPerFile) {
    pDb->cfg.daysPerFile = pAlter->daysPerFile;
    terrno = 0;
  }

  if (pAlter->daysToKeep0 > 0 && pAlter->daysToKeep0 != pDb->cfg.daysToKeep0) {
    pDb->cfg.daysToKeep0 = pAlter->daysToKeep0;
    terrno = 0;
  }

  if (pAlter->daysToKeep1 > 0 && pAlter->daysToKeep1 != pDb->cfg.daysToKeep1) {
    pDb->cfg.daysToKeep1 = pAlter->daysToKeep1;
    terrno = 0;
  }

  if (pAlter->daysToKeep2 > 0 && pAlter->daysToKeep2 != pDb->cfg.daysToKeep2) {
    pDb->cfg.daysToKeep2 = pAlter->daysToKeep2;
    terrno = 0;
  }

  if (pAlter->keepTimeOffset >= 0 && pAlter->keepTimeOffset != pDb->cfg.keepTimeOffset) {
    pDb->cfg.keepTimeOffset = pAlter->keepTimeOffset;
    terrno = 0;
  }

  if (pAlter->walFsyncPeriod >= 0 && pAlter->walFsyncPeriod != pDb->cfg.walFsyncPeriod) {
    pDb->cfg.walFsyncPeriod = pAlter->walFsyncPeriod;
    terrno = 0;
  }

  if (pAlter->walLevel >= 0 && pAlter->walLevel != pDb->cfg.walLevel) {
    pDb->cfg.walLevel = pAlter->walLevel;
    terrno = 0;
  }

  if (pAlter->strict >= 0 && pAlter->strict != pDb->cfg.strict) {
#if 1
    terrno = TSDB_CODE_OPS_NOT_SUPPORT;
#else
    pDb->cfg.strict = pAlter->strict;
    terrno = 0;
#endif
  }

  if (pAlter->cacheLast >= 0 && pAlter->cacheLast != pDb->cfg.cacheLast) {
    pDb->cfg.cacheLast = pAlter->cacheLast;
    terrno = 0;
  }

  if (pAlter->cacheLastSize > 0 && pAlter->cacheLastSize != pDb->cfg.cacheLastSize) {
    pDb->cfg.cacheLastSize = pAlter->cacheLastSize;
    terrno = 0;
  }

  if (pAlter->replications > 0 && pAlter->replications != pDb->cfg.replications) {
    pDb->cfg.replications = pAlter->replications;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->sstTrigger > 0 && pAlter->sstTrigger != pDb->cfg.sstTrigger) {
    pDb->cfg.sstTrigger = pAlter->sstTrigger;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->minRows > 0 && pAlter->minRows != pDb->cfg.minRows) {
    pDb->cfg.minRows = pAlter->minRows;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->walRetentionPeriod > TSDB_DB_MIN_WAL_RETENTION_PERIOD &&
      pAlter->walRetentionPeriod != pDb->cfg.walRetentionPeriod) {
    pDb->cfg.walRetentionPeriod = pAlter->walRetentionPeriod;
    pDb->vgVersion++;
    terrno = 0;
  }

  if (pAlter->walRetentionSize > TSDB_DB_MIN_WAL_RETENTION_SIZE &&
      pAlter->walRetentionSize != pDb->cfg.walRetentionSize) {
    pDb->cfg.walRetentionSize = pAlter->walRetentionSize;
    pDb->vgVersion++;
    terrno = 0;
  }

  return terrno;
}

static int32_t mndSetAlterDbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOld);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendPrepareLog(pTrans, pRedoRaw) != 0) {
    sdbFreeRaw(pRedoRaw);
    return -1;
  }

  (void)sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndSetAlterDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNew);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    sdbFreeRaw(pCommitRaw);
    return -1;
  }

  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndSetAlterDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SArray *pArray = mndBuildDnodesArray(pMnode, 0);

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (mndVgroupInDb(pVgroup, pNewDb->uid)) {
      if (mndBuildAlterVgroupAction(pMnode, pTrans, pOldDb, pNewDb, pVgroup, pArray) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        taosArrayDestroy(pArray);
        return -1;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  taosArrayDestroy(pArray);
  return 0;
}

static int32_t mndAlterDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pOld, SDbObj *pNew) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "alter-db");
  if (pTrans == NULL) return -1;
  mInfo("trans:%d, used to alter db:%s", pTrans->id, pOld->name);

  int32_t code = -1;
  mndTransSetDbName(pTrans, pOld->name, NULL);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  if (mndSetAlterDbPrepareLogs(pMnode, pTrans, pOld, pNew) != 0) goto _OVER;
  if (mndSetAlterDbCommitLogs(pMnode, pTrans, pOld, pNew) != 0) goto _OVER;
  if (mndSetAlterDbRedoActions(pMnode, pTrans, pOld, pNew) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessAlterDbReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SDbObj     *pDb = NULL;
  SAlterDbReq alterReq = {0};
  SDbObj      dbObj = {0};

  if (tDeserializeSAlterDbReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to alter", alterReq.db);

  pDb = mndAcquireDb(pMnode, alterReq.db);
  if (pDb == NULL) {
    goto _OVER;
  }

  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_ALTER_DB, pDb) != 0) {
    goto _OVER;
  }

  int32_t numOfTopics = 0;
  if (mndGetNumOfTopics(pMnode, pDb->name, &numOfTopics) != 0) {
    goto _OVER;
  }

  if (numOfTopics != 0 && alterReq.walRetentionPeriod == 0) {
    terrno = TSDB_CODE_MND_DB_RETENTION_PERIOD_ZERO;
    mError("db:%s, not allowed to set WAL_RETENTION_PERIOD 0 when there are topics defined. numOfTopics:%d", pDb->name,
           numOfTopics);
    goto _OVER;
  }

  memcpy(&dbObj, pDb, sizeof(SDbObj));
  if (dbObj.cfg.pRetensions != NULL) {
    dbObj.cfg.pRetensions = taosArrayDup(pDb->cfg.pRetensions, NULL);
    if (dbObj.cfg.pRetensions == NULL) goto _OVER;
  }

  code = mndSetDbCfgFromAlterDbReq(&dbObj, &alterReq);
  if (code != 0) {
    if (code == TSDB_CODE_MND_DB_OPTION_UNCHANGED) code = 0;
    goto _OVER;
  }

  code = mndCheckInChangeDbCfg(pMnode, &dbObj.cfg);
  if (code != 0) goto _OVER;

  dbObj.cfgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  code = mndAlterDb(pMnode, pReq, pDb, &dbObj);

  if (dbObj.cfg.replications != pDb->cfg.replications) {
    // return quickly, operation executed asynchronously
    mInfo("db:%s, alter db replica from %d to %d", pDb->name, pDb->cfg.replications, dbObj.cfg.replications);
  } else {
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  SName name = {0};
  tNameFromString(&name, alterReq.db, T_NAME_ACCT | T_NAME_DB);

  auditRecord(pReq, pMnode->clusterId, "alterDB", name.dbname, "", alterReq.sql, alterReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (terrno != 0) code = terrno;
    mError("db:%s, failed to alter since %s", alterReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  taosArrayDestroy(dbObj.cfg.pRetensions);
  tFreeSAlterDbReq(&alterReq);

  terrno = code;
  return code;
}

static void mndDumpDbCfgInfo(SDbCfgRsp *cfgRsp, SDbObj *pDb) {
  strcpy(cfgRsp->db, pDb->name);
  cfgRsp->dbId = pDb->uid;
  cfgRsp->cfgVersion = pDb->cfgVersion;
  cfgRsp->numOfVgroups = pDb->cfg.numOfVgroups;
  cfgRsp->numOfStables = pDb->cfg.numOfStables;
  cfgRsp->buffer = pDb->cfg.buffer;
  cfgRsp->cacheSize = pDb->cfg.cacheLastSize;
  cfgRsp->pageSize = pDb->cfg.pageSize;
  cfgRsp->pages = pDb->cfg.pages;
  cfgRsp->daysPerFile = pDb->cfg.daysPerFile;
  cfgRsp->daysToKeep0 = pDb->cfg.daysToKeep0;
  cfgRsp->daysToKeep1 = pDb->cfg.daysToKeep1;
  cfgRsp->daysToKeep2 = pDb->cfg.daysToKeep2;
  cfgRsp->keepTimeOffset = pDb->cfg.keepTimeOffset;
  cfgRsp->minRows = pDb->cfg.minRows;
  cfgRsp->maxRows = pDb->cfg.maxRows;
  cfgRsp->walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  cfgRsp->hashPrefix = pDb->cfg.hashPrefix;
  cfgRsp->hashSuffix = pDb->cfg.hashSuffix;
  cfgRsp->walLevel = pDb->cfg.walLevel;
  cfgRsp->precision = pDb->cfg.precision;
  cfgRsp->compression = pDb->cfg.compression;
  cfgRsp->replications = pDb->cfg.replications;
  cfgRsp->strict = pDb->cfg.strict;
  cfgRsp->cacheLast = pDb->cfg.cacheLast;
  cfgRsp->tsdbPageSize = pDb->cfg.tsdbPageSize;
  cfgRsp->walRetentionPeriod = pDb->cfg.walRetentionPeriod;
  cfgRsp->walRollPeriod = pDb->cfg.walRollPeriod;
  cfgRsp->walRetentionSize = pDb->cfg.walRetentionSize;
  cfgRsp->walSegmentSize = pDb->cfg.walSegmentSize;
  cfgRsp->numOfRetensions = pDb->cfg.numOfRetensions;
  cfgRsp->pRetensions = taosArrayDup(pDb->cfg.pRetensions, NULL);
  cfgRsp->schemaless = pDb->cfg.schemaless;
  cfgRsp->sstTrigger = pDb->cfg.sstTrigger;
}

static int32_t mndProcessGetDbCfgReq(SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SDbCfgReq cfgReq = {0};
  SDbCfgRsp cfgRsp = {0};

  if (tDeserializeSDbCfgReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  if (strcasecmp(cfgReq.db, TSDB_INFORMATION_SCHEMA_DB) && strcasecmp(cfgReq.db, TSDB_PERFORMANCE_SCHEMA_DB)) {
    pDb = mndAcquireDb(pMnode, cfgReq.db);
    if (pDb == NULL) {
      goto _OVER;
    }

    mndDumpDbCfgInfo(&cfgRsp, pDb);
  }

  int32_t contLen = tSerializeSDbCfgRsp(NULL, 0, &cfgRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  tSerializeSDbCfgRsp(pRsp, contLen, &cfgRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  code = 0;

_OVER:

  tFreeSDbCfgRsp(&cfgRsp);

  if (code != 0) {
    mError("db:%s, failed to get cfg since %s", cfgReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);

  return code;
}

static int32_t mndSetDropDbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pDb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendPrepareLog(pTrans, pRedoRaw) != 0) return -1;
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
      (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED);
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
      (void)sdbSetRawStatus(pStbRaw, SDB_STATUS_DROPPED);
    }

    sdbRelease(pSdb, pStb);
  }

  return 0;
}

static int32_t mndBuildDropVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
    if (mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, true) != 0) {
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

static int32_t mndDropDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-db");
  if (pTrans == NULL) {
    goto _OVER;
  }

  mInfo("trans:%d start to drop db:%s", pTrans->id, pDb->name);

  mndTransSetDbName(pTrans, pDb->name, NULL);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    goto _OVER;
  }

  if (mndTopicExistsForDb(pMnode, pDb)) {
    terrno = TSDB_CODE_MND_TOPIC_MUST_BE_DELETED;
    goto _OVER;
  }

  if (mndSetDropDbPrepareLogs(pMnode, pTrans, pDb) != 0) goto _OVER;
  if (mndSetDropDbCommitLogs(pMnode, pTrans, pDb) != 0) goto _OVER;
  /*if (mndDropOffsetByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  /*if (mndDropSubByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  /*if (mndDropTopicByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  if (mndDropStreamByDb(pMnode, pTrans, pDb) != 0) goto _OVER;
#ifdef TD_ENTERPRISE
  if (mndDropViewByDb(pMnode, pTrans, pDb) != 0) goto _OVER;
#endif  
  if (mndDropSmasByDb(pMnode, pTrans, pDb) != 0) goto _OVER;
  if (mndDropIdxsByDb(pMnode, pTrans, pDb) != 0) goto _OVER;
  if (mndSetDropDbRedoActions(pMnode, pTrans, pDb) != 0) goto _OVER;
  if (mndUserRemoveDb(pMnode, pTrans, pDb->name) != 0) goto _OVER;

  int32_t rspLen = 0;
  void   *pRsp = NULL;
  if (mndBuildDropDbRsp(pDb, &rspLen, &pRsp, false) < 0) goto _OVER;
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropDbReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  SDropDbReq dropReq = {0};

  if (tDeserializeSDropDbReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to drop", dropReq.db);

  pDb = mndAcquireDb(pMnode, dropReq.db);
  if (pDb == NULL) {
    if (dropReq.ignoreNotExists) {
      code = mndBuildDropDbRsp(pDb, &pReq->info.rspLen, &pReq->info.rsp, true);
    }
    goto _OVER;
  }

  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_DB, pDb) != 0) {
    goto _OVER;
  }

  code = mndDropDb(pMnode, pReq, pDb);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  SName name = {0};
  tNameFromString(&name, dropReq.db, T_NAME_ACCT | T_NAME_DB);

  auditRecord(pReq, pMnode->clusterId, "dropDB", name.dbname, "", dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop since %s", dropReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSDropDbReq(&dropReq);
  return code;
}

static int32_t mndGetDBTableNum(SDbObj *pDb, SMnode *pMnode) {
  int32_t numOfTables = 0;
  int32_t vindex = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (vindex < pDb->cfg.numOfVgroups) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (mndVgroupInDb(pVgroup, pDb->uid)) {
      numOfTables += pVgroup->numOfTables / TSDB_TABLE_NUM_UNIT;
      vindex++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  sdbCancelFetch(pSdb, pIter);
  return numOfTables;
}

static void mndBuildDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList) {
  int32_t vindex = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if ((NULL == pDb || pVgroup->dbUid == pDb->uid) && !pVgroup->isTsma) {
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
        if (pVgid->syncState == TAOS_SYNC_STATE_LEADER) {
          vgInfo.epSet.inUse = gid;
        }
      }
      vindex++;
      taosArrayPush(pVgList, &vgInfo);
    }

    sdbRelease(pSdb, pVgroup);

    if (pDb && (vindex >= pDb->cfg.numOfVgroups)) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }
}

int32_t mndExtractDbInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq) {
  pRsp->pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
  if (pRsp->pVgroupInfos == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

  if (pReq == NULL || pReq->vgVersion < pDb->vgVersion || pReq->dbId != pDb->uid || numOfTable != pReq->numOfTable ||
      pReq->stateTs < pDb->stateTs) {
    mndBuildDBVgroupInfo(pDb, pMnode, pRsp->pVgroupInfos);
  }

  memcpy(pRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
  pRsp->uid = pDb->uid;
  pRsp->vgVersion = pDb->vgVersion;
  pRsp->stateTs = pDb->stateTs;
  pRsp->vgNum = taosArrayGetSize(pRsp->pVgroupInfos);
  pRsp->hashMethod = pDb->cfg.hashMethod;
  pRsp->hashPrefix = pDb->cfg.hashPrefix;
  pRsp->hashSuffix = pDb->cfg.hashSuffix;
  return 0;
}

static int32_t mndProcessUseDbReq(SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SUseDbReq usedbReq = {0};
  SUseDbRsp usedbRsp = {0};

  if (tDeserializeSUseDbReq(pReq->pCont, pReq->contLen, &usedbReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  char *p = strchr(usedbReq.db, '.');
  if (p && ((0 == strcmp(p + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(p + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
    memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
    int32_t vgVersion = mndGetGlobalVgroupVersion(pMnode);
    if (usedbReq.vgVersion < vgVersion) {
      usedbRsp.pVgroupInfos = taosArrayInit(10, sizeof(SVgroupInfo));
      if (usedbRsp.pVgroupInfos == NULL) goto _OVER;

      mndBuildDBVgroupInfo(NULL, pMnode, usedbRsp.pVgroupInfos);
      usedbRsp.vgVersion = vgVersion++;
    } else {
      usedbRsp.vgVersion = usedbReq.vgVersion;
    }
    usedbRsp.vgNum = taosArrayGetSize(usedbRsp.pVgroupInfos);
    code = 0;
  } else {
    pDb = mndAcquireDb(pMnode, usedbReq.db);
    if (pDb == NULL) {
      memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
      usedbRsp.uid = usedbReq.dbId;
      usedbRsp.vgVersion = usedbReq.vgVersion;
      usedbRsp.errCode = terrno;

      if (terrno == TSDB_CODE_MND_DB_IN_CREATING) {
        code = terrno;
        goto _OVER;
      }
    } else {
      if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_USE_DB, pDb) != 0) {
        goto _OVER;
      }

      if (mndExtractDbInfo(pMnode, pDb, &usedbRsp, &usedbReq) < 0) {
        goto _OVER;
      }

      mDebug("db:%s, process usedb req vgVersion:%d stateTs:%" PRId64 ", rsp vgVersion:%d stateTs:%" PRId64,
             usedbReq.db, usedbReq.vgVersion, usedbReq.stateTs, usedbRsp.vgVersion, usedbRsp.stateTs);
      code = 0;
    }
  }

  int32_t contLen = tSerializeSUseDbRsp(NULL, 0, &usedbRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  tSerializeSUseDbRsp(pRsp, contLen, &usedbRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process use db req since %s", usedbReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSUsedbRsp(&usedbRsp);

  return code;
}

int32_t mndValidateDbInfo(SMnode *pMnode, SDbCacheInfo *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen) {
  SDbHbBatchRsp batchRsp = {0};
  batchRsp.pArray = taosArrayInit(numOfDbs, sizeof(SDbHbRsp));
  if (batchRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfDbs; ++i) {
    SDbCacheInfo *pDbCacheInfo = &pDbs[i];
    pDbCacheInfo->dbId = be64toh(pDbCacheInfo->dbId);
    pDbCacheInfo->vgVersion = htonl(pDbCacheInfo->vgVersion);
    pDbCacheInfo->cfgVersion = htonl(pDbCacheInfo->cfgVersion);
    pDbCacheInfo->numOfTable = htonl(pDbCacheInfo->numOfTable);
    pDbCacheInfo->stateTs = be64toh(pDbCacheInfo->stateTs);

    SDbHbRsp rsp = {0};

    if ((0 == strcasecmp(pDbCacheInfo->dbFName, TSDB_INFORMATION_SCHEMA_DB) ||
         (0 == strcasecmp(pDbCacheInfo->dbFName, TSDB_PERFORMANCE_SCHEMA_DB)))) {
      int32_t vgVersion = mndGetGlobalVgroupVersion(pMnode);
      if (pDbCacheInfo->vgVersion >= vgVersion) {
        continue;
      }

      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      memcpy(rsp.useDbRsp->db, pDbCacheInfo->dbFName, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->pVgroupInfos = taosArrayInit(10, sizeof(SVgroupInfo));

      mndBuildDBVgroupInfo(NULL, pMnode, rsp.useDbRsp->pVgroupInfos);
      rsp.useDbRsp->vgVersion = vgVersion++;

      rsp.useDbRsp->vgNum = taosArrayGetSize(rsp.useDbRsp->pVgroupInfos);

      taosArrayPush(batchRsp.pArray, &rsp);

      continue;
    }

    SDbObj *pDb = mndAcquireDb(pMnode, pDbCacheInfo->dbFName);
    if (pDb == NULL) {
      mTrace("db:%s, no exist", pDbCacheInfo->dbFName);
      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      memcpy(rsp.useDbRsp->db, pDbCacheInfo->dbFName, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->uid = pDbCacheInfo->dbId;
      rsp.useDbRsp->vgVersion = -1;
      taosArrayPush(batchRsp.pArray, &rsp);
      continue;
    }

    int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

    if (pDbCacheInfo->vgVersion >= pDb->vgVersion &&
        pDbCacheInfo->cfgVersion >= pDb->cfgVersion &&
        numOfTable == pDbCacheInfo->numOfTable &&
        pDbCacheInfo->stateTs == pDb->stateTs) {
      mTrace("db:%s, valid dbinfo, vgVersion:%d cfgVersion:%d stateTs:%" PRId64
             " numOfTables:%d, not changed vgVersion:%d cfgVersion:%d stateTs:%" PRId64 " numOfTables:%d",
             pDbCacheInfo->dbFName, pDbCacheInfo->vgVersion, pDbCacheInfo->cfgVersion, pDbCacheInfo->stateTs, pDbCacheInfo->numOfTable,
             pDb->vgVersion, pDb->cfgVersion, pDb->stateTs, numOfTable);
      mndReleaseDb(pMnode, pDb);
      continue;
    } else {
      mInfo("db:%s, valid dbinfo, vgVersion:%d cfgVersion:%d stateTs:%" PRId64
            " numOfTables:%d, changed to vgVersion:%d cfgVersion:%d stateTs:%" PRId64 " numOfTables:%d",
            pDbCacheInfo->dbFName, pDbCacheInfo->vgVersion, pDbCacheInfo->cfgVersion, pDbCacheInfo->stateTs, pDbCacheInfo->numOfTable,
            pDb->vgVersion, pDb->cfgVersion, pDb->stateTs, numOfTable);
    }

    if (pDbCacheInfo->cfgVersion < pDb->cfgVersion) {
      rsp.cfgRsp = taosMemoryCalloc(1, sizeof(SDbCfgRsp));
      mndDumpDbCfgInfo(rsp.cfgRsp, pDb);
    }

    if (pDbCacheInfo->vgVersion < pDb->vgVersion ||
        numOfTable != pDbCacheInfo->numOfTable ||
        pDbCacheInfo->stateTs != pDb->stateTs) {
      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      rsp.useDbRsp->pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
      if (rsp.useDbRsp->pVgroupInfos == NULL) {
        mndReleaseDb(pMnode, pDb);
        mError("db:%s, failed to malloc usedb response", pDb->name);
        continue;
      }

      mndBuildDBVgroupInfo(pDb, pMnode, rsp.useDbRsp->pVgroupInfos);
      memcpy(rsp.useDbRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->uid = pDb->uid;
      rsp.useDbRsp->vgVersion = pDb->vgVersion;
      rsp.useDbRsp->stateTs = pDb->stateTs;
      rsp.useDbRsp->vgNum = (int32_t)taosArrayGetSize(rsp.useDbRsp->pVgroupInfos);
      rsp.useDbRsp->hashMethod = pDb->cfg.hashMethod;
      rsp.useDbRsp->hashPrefix = pDb->cfg.hashPrefix;
      rsp.useDbRsp->hashSuffix = pDb->cfg.hashSuffix;
    }

    taosArrayPush(batchRsp.pArray, &rsp);
    mndReleaseDb(pMnode, pDb);
  }

  int32_t rspLen = tSerializeSDbHbBatchRsp(NULL, 0, &batchRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeSDbHbBatchRsp(&batchRsp);
    return -1;
  }
  tSerializeSDbHbBatchRsp(pRsp, rspLen, &batchRsp);

  *ppRsp = pRsp;
  *pRspLen = rspLen;

  tFreeSDbHbBatchRsp(&batchRsp);
  return 0;
}

static int32_t mndTrimDb(SMnode *pMnode, SDbObj *pDb) {
  SSdb       *pSdb = pMnode->pSdb;
  SVgObj     *pVgroup = NULL;
  void       *pIter = NULL;
  SVTrimDbReq trimReq = {.timestamp = taosGetTimestampSec()};
  int32_t     reqLen = tSerializeSVTrimDbReq(NULL, 0, &trimReq);
  int32_t     contLen = reqLen + sizeof(SMsgHead);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbCancelFetch(pSdb, pVgroup);
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    tSerializeSVTrimDbReq((char *)pHead + sizeof(SMsgHead), contLen, &trimReq);

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_TRIM, .pCont = pHead, .contLen = contLen};
    SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
    int32_t code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, failed to send vnode-trim request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mInfo("vgId:%d, send vnode-trim request to vnode, time:%d", pVgroup->vgId, trimReq.timestamp);
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndProcessTrimDbReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  STrimDbReq trimReq = {0};

  if (tDeserializeSTrimDbReq(pReq->pCont, pReq->contLen, &trimReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("db:%s, start to trim", trimReq.db);

  pDb = mndAcquireDb(pMnode, trimReq.db);
  if (pDb == NULL) {
    goto _OVER;
  }

  if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_TRIM_DB, pDb) != 0) {
    goto _OVER;
  }

  code = mndTrimDb(pMnode, pDb);

_OVER:
  if (code != 0) {
    mError("db:%s, failed to process trim db req since %s", trimReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  return code;
}

const char *mndGetDbStr(const char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;
  if (pos == NULL) return src;
  return pos;
}

const char *mndGetStableStr(const char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;
  if (pos == NULL) return src;
  return mndGetDbStr(pos);
}

static int64_t getValOfDiffPrecision(int8_t unit, int64_t val) {
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

static char *buildRetension(SArray *pRetension) {
  size_t size = taosArrayGetSize(pRetension);
  if (size == 0) {
    return NULL;
  }

  char       *p1 = taosMemoryCalloc(1, 100);
  SRetention *p = taosArrayGet(pRetension, 0);

  int32_t len = 2;

  int64_t v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
  int64_t v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
  len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);

  if (size > 1) {
    len += sprintf(p1 + len, ",");
    p = taosArrayGet(pRetension, 1);

    v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
  }

  if (size > 2) {
    len += sprintf(p1 + len, ",");
    p = taosArrayGet(pRetension, 2);

    v1 = getValOfDiffPrecision(p->freqUnit, p->freq);
    v2 = getValOfDiffPrecision(p->keepUnit, p->keep);
    len += sprintf(p1 + len, "%" PRId64 "%c:%" PRId64 "%c", v1, p->freqUnit, v2, p->keepUnit);
  }

  varDataSetLen(p1, len);
  return p1;
}

static const char *getCacheModelStr(int8_t cacheModel) {
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
  return "unknown";
}

bool mndIsDbReady(SMnode *pMnode, SDbObj *pDb) {
  if (pDb->cfg.replications == 1) return true;

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  bool  isReady = true;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid && pVgroup->replica > 1) {
      bool hasLeader = false;
      for (int32_t i = 0; i < pVgroup->replica; ++i) {
        if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER) {
          hasLeader = true;
        }
      }
      if (!hasLeader) isReady = false;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return isReady;
}

static void mndDumpDbInfoData(SMnode *pMnode, SSDataBlock *pBlock, SDbObj *pDb, SShowObj *pShow, int32_t rows,
                              int64_t numOfTables, bool sysDb, ESdbStatus objStatus, bool sysinfo) {
  int32_t cols = 0;
  int32_t bytes = pShow->pMeta->pSchemas[cols].bytes;
  char   *buf = taosMemoryMalloc(bytes);

  const char *name = mndGetDbStr(pDb->name);
  if (name != NULL) {
    STR_WITH_MAXSIZE_TO_VARSTR(buf, name, bytes);
  } else {
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "NULL", bytes);
  }

  const char *precStr = NULL;
  switch (pDb->cfg.precision) {
    case TSDB_TIME_PRECISION_MILLI:
      precStr = TSDB_TIME_PRECISION_MILLI_STR;
      break;
    case TSDB_TIME_PRECISION_MICRO:
      precStr = TSDB_TIME_PRECISION_MICRO_STR;
      break;
    case TSDB_TIME_PRECISION_NANO:
      precStr = TSDB_TIME_PRECISION_NANO_STR;
      break;
    default:
      precStr = "none";
      break;
  }
  char precVstr[10] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(precVstr, precStr, 10);

  char *statusStr = "ready";
  if (objStatus == SDB_STATUS_CREATING) {
    statusStr = "creating";
  } else if (objStatus == SDB_STATUS_DROPPING) {
    statusStr = "dropping";
  } else {
    if (!sysDb && !mndIsDbReady(pMnode, pDb)) {
      statusStr = "unsynced";
    }
  }
  char statusVstr[24] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(statusVstr, statusStr, 24);

  if (sysDb || !sysinfo) {
    for (int32_t i = 0; i < pShow->numOfColumns; ++i) {
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      if (i == 0) {
        colDataSetVal(pColInfo, rows, buf, false);
      } else if (i == 1) {
        colDataSetVal(pColInfo, rows, (const char *)&pDb->createdTime, false);
      } else if (i == 3) {
        colDataSetVal(pColInfo, rows, (const char *)&numOfTables, false);
      } else if (i == 14) {
        colDataSetVal(pColInfo, rows, precVstr, false);
      } else if (i == 15) {
        colDataSetVal(pColInfo, rows, statusVstr, false);
      } else {
        colDataSetNULL(pColInfo, rows);
      }
    }
  } else {
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.numOfVgroups, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&numOfTables, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.replications, false);

    const char *strictStr = pDb->cfg.strict ? "on" : "off";
    char        strictVstr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(strictVstr, strictStr, 24);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)strictVstr, false);

    char    durationVstr[128] = {0};
    int32_t len = sprintf(&durationVstr[VARSTR_HEADER_SIZE], "%dm", pDb->cfg.daysPerFile);
    varDataSetLen(durationVstr, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)durationVstr, false);

    char keepVstr[128] = {0};
    if (pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep1 || pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep2) {
      len = sprintf(&keepVstr[VARSTR_HEADER_SIZE], "%dm,%dm,%dm", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2,
                    pDb->cfg.daysToKeep0);
    } else {
      len = sprintf(&keepVstr[VARSTR_HEADER_SIZE], "%dm,%dm,%dm", pDb->cfg.daysToKeep0, pDb->cfg.daysToKeep1,
                    pDb->cfg.daysToKeep2);
    }
    varDataSetLen(keepVstr, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)keepVstr, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.buffer, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.pageSize, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.pages, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.minRows, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.maxRows, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.compression, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)precVstr, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)statusVstr, false);

    char *rentensionVstr = buildRetension(pDb->cfg.pRetensions);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (rentensionVstr == NULL) {
      colDataSetNULL(pColInfo, rows);
    } else {
      colDataSetVal(pColInfo, rows, (const char *)rentensionVstr, false);
      taosMemoryFree(rentensionVstr);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.numOfStables, false);

    const char *cacheModelStr = getCacheModelStr(pDb->cfg.cacheLast);
    char        cacheModelVstr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(cacheModelVstr, cacheModelStr, 24);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)cacheModelVstr, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.cacheLastSize, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walLevel, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walFsyncPeriod, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walRetentionPeriod, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walRetentionSize, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.sstTrigger, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int16_t hashPrefix = pDb->cfg.hashPrefix;
    if (hashPrefix > 0) {
      hashPrefix = pDb->cfg.hashPrefix - strlen(pDb->name) - 1;
    } else if (hashPrefix < 0) {
      hashPrefix = pDb->cfg.hashPrefix + strlen(pDb->name) + 1;
    }
    colDataSetVal(pColInfo, rows, (const char *)&hashPrefix, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.hashSuffix, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.tsdbPageSize, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.keepTimeOffset, false);
  }

  taosMemoryFree(buf);
}

static void setInformationSchemaDbCfg(SMnode *pMnode, SDbObj *pDbObj) {
  tstrncpy(pDbObj->name, TSDB_INFORMATION_SCHEMA_DB, tListLen(pDbObj->name));
  pDbObj->createdTime = mndGetClusterCreateTime(pMnode);
  pDbObj->cfg.numOfVgroups = 0;
  pDbObj->cfg.strict = 1;
  pDbObj->cfg.replications = 1;
  pDbObj->cfg.precision = TSDB_TIME_PRECISION_MILLI;
}

static void setPerfSchemaDbCfg(SMnode *pMnode, SDbObj *pDbObj) {
  tstrncpy(pDbObj->name, TSDB_PERFORMANCE_SCHEMA_DB, tListLen(pDbObj->name));
  pDbObj->createdTime = mndGetClusterCreateTime(pMnode);
  pDbObj->cfg.numOfVgroups = 0;
  pDbObj->cfg.strict = 1;
  pDbObj->cfg.replications = 1;
  pDbObj->cfg.precision = TSDB_TIME_PRECISION_MILLI;
}

static bool mndGetTablesOfDbFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int32_t *numOfTables = p1;
  int64_t  uid = *(int64_t *)p2;
  if (pVgroup->dbUid == uid) {
    *numOfTables += pVgroup->numOfTables;
  }
  return true;
}

static int32_t mndRetrieveDbs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  SDbObj    *pDb = NULL;
  ESdbStatus objStatus = 0;

  SUserObj *pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pUser == NULL) return 0;
  bool sysinfo = pUser->sysInfo;

  // Append the information_schema database into the result.
  if (!pShow->sysDbRsp) {
    SDbObj infoschemaDb = {0};
    setInformationSchemaDbCfg(pMnode, &infoschemaDb);
    size_t numOfTables = 0;
    getVisibleInfosTablesNum(sysinfo, &numOfTables);
    mndDumpDbInfoData(pMnode, pBlock, &infoschemaDb, pShow, numOfRows, numOfTables, true, 0, 1);

    numOfRows += 1;

    SDbObj perfschemaDb = {0};
    setPerfSchemaDbCfg(pMnode, &perfschemaDb);
    numOfTables = 0;
    getPerfDbMeta(NULL, &numOfTables);
    mndDumpDbInfoData(pMnode, pBlock, &perfschemaDb, pShow, numOfRows, numOfTables, true, 0, 1);

    numOfRows += 1;
    pShow->sysDbRsp = true;
  }

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DB, pShow->pIter, (void **)&pDb, &objStatus, true);
    if (pShow->pIter == NULL) break;

    if (mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_OR_WRITE_DB, pDb) == 0) {
      int32_t numOfTables = 0;
      sdbTraverse(pSdb, SDB_VGROUP, mndGetTablesOfDbFp, &numOfTables, &pDb->uid, NULL);
      mndDumpDbInfoData(pMnode, pBlock, pDb, pShow, numOfRows, numOfTables, false, objStatus, sysinfo);
      numOfRows++;
    }

    sdbRelease(pSdb, pDb);
  }

  pShow->numOfRows += numOfRows;
  mndReleaseUser(pMnode, pUser);
  return numOfRows;
}

static void mndCancelGetNextDb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
