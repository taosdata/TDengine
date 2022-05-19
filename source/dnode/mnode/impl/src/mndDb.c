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
#include "mndOffset.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndStb.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "systable.h"

#define DB_VER_NUMBER   1
#define DB_RESERVE_SIZE 64

static SSdbRaw *mndDbActionEncode(SDbObj *pDb);
static SSdbRow *mndDbActionDecode(SSdbRaw *pRaw);
static int32_t  mndDbActionInsert(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionDelete(SSdb *pSdb, SDbObj *pDb);
static int32_t  mndDbActionUpdate(SSdb *pSdb, SDbObj *pOld, SDbObj *pNew);
static int32_t  mndProcessCreateDbReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterDbReq(SRpcMsg *pReq);
static int32_t  mndProcessDropDbReq(SRpcMsg *pReq);
static int32_t  mndProcessUseDbReq(SRpcMsg *pReq);
static int32_t  mndProcessCompactDbReq(SRpcMsg *pReq);
static int32_t  mndRetrieveDbs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity);
static void     mndCancelGetNextDb(SMnode *pMnode, void *pIter);
static int32_t  mndProcessGetDbCfgReq(SRpcMsg *pReq);

int32_t mndInitDb(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_DB,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndDbActionEncode,
      .decodeFp = (SdbDecodeFp)mndDbActionDecode,
      .insertFp = (SdbInsertFp)mndDbActionInsert,
      .updateFp = (SdbUpdateFp)mndDbActionUpdate,
      .deleteFp = (SdbDeleteFp)mndDbActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DB, mndProcessCreateDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_DB, mndProcessAlterDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DB, mndProcessDropDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_USE_DB, mndProcessUseDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMPACT_DB, mndProcessCompactDbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_DB_CFG, mndProcessGetDbCfgReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveDbs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextDb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDb(SMnode *pMnode) {}

static SSdbRaw *mndDbActionEncode(SDbObj *pDb) {
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
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRows, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRows, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.fsyncPeriod, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.strict, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLastRow, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.hashMethod, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfRetensions, _OVER)
  for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
    TASSERT(taosArrayGetSize(pDb->cfg.pRetensions) == pDb->cfg.numOfRetensions);
    SRetention *pRetension = taosArrayGet(pDb->cfg.pRetensions, i);
    SDB_SET_INT64(pRaw, dataPos, pRetension->freq, _OVER)
    SDB_SET_INT64(pRaw, dataPos, pRetension->keep, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->freqUnit, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pRetension->keepUnit, _OVER)
  }

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

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != DB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SDbObj));
  if (pRow == NULL) goto _OVER;

  SDbObj *pDb = sdbGetRowObj(pRow);
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
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysPerFile, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep0, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep1, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep2, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.minRows, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.maxRows, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.fsyncPeriod, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.walLevel, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.precision, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.compression, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.replications, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.strict, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.cacheLastRow, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.hashMethod, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfRetensions, _OVER)
  if (pDb->cfg.numOfRetensions > 0) {
    pDb->cfg.pRetensions = taosArrayInit(pDb->cfg.numOfRetensions, sizeof(SRetention));
    if (pDb->cfg.pRetensions == NULL) goto _OVER;
    for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
      SRetention retension = {0};
      SDB_GET_INT64(pRaw, dataPos, &retension.freq, _OVER)
      SDB_GET_INT64(pRaw, dataPos, &retension.keep, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &retension.freqUnit, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &retension.keepUnit, _OVER)
      if (taosArrayPush(pDb->cfg.pRetensions, &retension) == NULL) {
        goto _OVER;
      }
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, DB_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pDb->lock);

  terrno = 0;

_OVER:
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
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->cfgVersion = pNew->cfgVersion;
  pOld->vgVersion = pNew->vgVersion;
  pOld->cfg.buffer = pNew->cfg.buffer;
  pOld->cfg.pages = pNew->cfg.pages;
  pOld->cfg.pageSize = pNew->cfg.pageSize;
  pOld->cfg.daysPerFile = pNew->cfg.daysPerFile;
  pOld->cfg.daysToKeep0 = pNew->cfg.daysToKeep0;
  pOld->cfg.daysToKeep1 = pNew->cfg.daysToKeep1;
  pOld->cfg.daysToKeep2 = pNew->cfg.daysToKeep2;
  pOld->cfg.fsyncPeriod = pNew->cfg.fsyncPeriod;
  pOld->cfg.walLevel = pNew->cfg.walLevel;
  pOld->cfg.strict = pNew->cfg.strict;
  pOld->cfg.cacheLastRow = pNew->cfg.cacheLastRow;
  pOld->cfg.replications = pNew->cfg.replications;
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
  if (pDb == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
  }
  return pDb;
}

void mndReleaseDb(SMnode *pMnode, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDb);
}

static int32_t mndAddCreateVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid) {
  STransAction action = {0};

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
  action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndAddAlterVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReq(pMnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_VNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndAddDropVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid,
                                     bool isRedo) {
  STransAction action = {0};

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
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (isRedo) {
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  } else {
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
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
  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return -1;
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return -1;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return -1;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return -1;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return -1;
  if (pCfg->minRows < TSDB_MIN_MINROWS_FBLOCK || pCfg->minRows > TSDB_MAX_MINROWS_FBLOCK) return -1;
  if (pCfg->maxRows < TSDB_MIN_MAXROWS_FBLOCK || pCfg->maxRows > TSDB_MAX_MAXROWS_FBLOCK) return -1;
  if (pCfg->minRows > pCfg->maxRows) return -1;
  if (pCfg->fsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->fsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return -1;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return -1;
  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) return -1;
  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) return -1;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA || pCfg->replications > TSDB_MAX_DB_REPLICA) return -1;
  if (pCfg->replications != 1 && pCfg->replications != 3) return -1;
  if (pCfg->strict < TSDB_DB_STRICT_OFF || pCfg->strict > TSDB_DB_STRICT_ON) return -1;
  if (pCfg->cacheLastRow < TSDB_MIN_DB_CACHE_LAST_ROW || pCfg->cacheLastRow > TSDB_MAX_DB_CACHE_LAST_ROW) return -1;
  if (pCfg->hashMethod != 1) return -1;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) {
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return -1;
  }

  terrno = 0;
  return TSDB_CODE_SUCCESS;
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
  if (pCfg->minRows < 0) pCfg->minRows = TSDB_DEFAULT_MINROWS_FBLOCK;
  if (pCfg->maxRows < 0) pCfg->maxRows = TSDB_DEFAULT_MAXROWS_FBLOCK;
  if (pCfg->fsyncPeriod < 0) pCfg->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  if (pCfg->walLevel < 0) pCfg->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  if (pCfg->precision < 0) pCfg->precision = TSDB_DEFAULT_PRECISION;
  if (pCfg->compression < 0) pCfg->compression = TSDB_DEFAULT_COMP_LEVEL;
  if (pCfg->replications < 0) pCfg->replications = TSDB_DEFAULT_DB_REPLICA;
  if (pCfg->strict < 0) pCfg->strict = TSDB_DEFAULT_DB_STRICT;
  if (pCfg->cacheLastRow < 0) pCfg->cacheLastRow = TSDB_DEFAULT_CACHE_LAST_ROW;
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
      .daysPerFile = pCreate->daysPerFile,
      .daysToKeep0 = pCreate->daysToKeep0,
      .daysToKeep1 = pCreate->daysToKeep1,
      .daysToKeep2 = pCreate->daysToKeep2,
      .minRows = pCreate->minRows,
      .maxRows = pCreate->maxRows,
      .fsyncPeriod = pCreate->fsyncPeriod,
      .walLevel = pCreate->walLevel,
      .precision = pCreate->precision,
      .compression = pCreate->compression,
      .replications = pCreate->replications,
      .strict = pCreate->strict,
      .cacheLastRow = pCreate->cacheLastRow,
      .hashMethod = 1,
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
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  SVgObj *pVgroups = NULL;
  if (mndAllocVgroup(pMnode, &dbObj, &pVgroups) != 0) {
    mError("db:%s, failed to create since %s", pCreate->db, terrstr());
    return -1;
  }

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_DB, pReq);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to create db:%s", pTrans->id, pCreate->db);

  mndTransSetDbInfo(pTrans, &dbObj);
  if (mndSetCreateDbRedoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbUndoLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbCommitLogs(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbRedoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndSetCreateDbUndoActions(pMnode, pTrans, &dbObj, pVgroups) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  taosMemoryFree(pVgroups);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateDbReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SUserObj    *pUser = NULL;
  SCreateDbReq createReq = {0};

  if (tDeserializeSCreateDbReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("db:%s, start to create, vgroups:%d", createReq.db, createReq.numOfVgroups);

  pDb = mndAcquireDb(pMnode, createReq.db);
  if (pDb != NULL) {
    if (createReq.ignoreExist) {
      mDebug("db:%s, already exist, ignore exist is set", createReq.db);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_DB_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_MND_DB_NOT_EXIST) {
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckCreateDbAuth(pUser) != 0) {
    goto _OVER;
  }

  code = mndCreateDb(pMnode, pReq, &createReq, pUser);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
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

  if (pAlter->fsyncPeriod >= 0 && pAlter->fsyncPeriod != pDb->cfg.fsyncPeriod) {
    pDb->cfg.fsyncPeriod = pAlter->fsyncPeriod;
    terrno = 0;
  }

  if (pAlter->walLevel >= 0 && pAlter->walLevel != pDb->cfg.walLevel) {
    pDb->cfg.walLevel = pAlter->walLevel;
    terrno = 0;
  }

  if (pAlter->strict >= 0 && pAlter->strict != pDb->cfg.strict) {
    pDb->cfg.strict = pAlter->strict;
    terrno = 0;
  }

  if (pAlter->cacheLastRow >= 0 && pAlter->cacheLastRow != pDb->cfg.cacheLastRow) {
    pDb->cfg.cacheLastRow = pAlter->cacheLastRow;
    terrno = 0;
  }

  if (pAlter->replications > 0 && pAlter->replications != pDb->cfg.replications) {
    pDb->cfg.replications = pAlter->replications;
    pDb->vgVersion++;
    terrno = 0;
  }

  return terrno;
}

static int32_t mndSetAlterDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOld);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetAlterDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNew);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndBuildAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SArray *pArray) {
  if (pVgroup->replica <= 0 || pVgroup->replica == pDb->cfg.replications) {
    if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, pVgroup) != 0) {
      return -1;
    }
  } else {
    SVgObj newVgroup = {0};
    memcpy(&newVgroup, pVgroup, sizeof(SVgObj));
    if (newVgroup.replica < pDb->cfg.replications) {
      mInfo("db:%s, vgId:%d, will add 2 vnodes, vn:0 dnode:%d", pVgroup->dbName, pVgroup->vgId,
            pVgroup->vnodeGid[0].dnodeId);

      if (mndAddVnodeToVgroup(pMnode, &newVgroup, pArray) != 0) {
        mError("db:%s, failed to add vnode to vgId:%d since %s", pDb->name, newVgroup.vgId, terrstr());
        return -1;
      }
      newVgroup.replica = pDb->cfg.replications;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVgroup, &newVgroup.vnodeGid[1]) != 0) return -1;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVgroup, &newVgroup.vnodeGid[2]) != 0) return -1;
    } else {
      mInfo("db:%s, vgId:%d, will remove 2 vnodes", pVgroup->dbName, pVgroup->vgId);

      SVnodeGid del1 = {0};
      SVnodeGid del2 = {0};
      if (mndRemoveVnodeFromVgroup(pMnode, &newVgroup, pArray, &del1, &del2) != 0) {
        mError("db:%s, failed to remove vnode from vgId:%d since %s", pDb->name, newVgroup.vgId, terrstr());
        return -1;
      }
      newVgroup.replica = pDb->cfg.replications;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVgroup, &del1, true) != 0) return -1;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVgroup, &del2, true) != 0) return -1;
    }

    SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) return -1;
    if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) return -1;
    if (sdbSetRawStatus(pVgRaw, SDB_STATUS_READY) != 0) return -1;
  }

  return 0;
}

static int32_t mndSetAlterDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SArray *pArray = mndBuildDnodesArray(pMnode);

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pNew->uid) {
      if (mndBuildAlterVgroupAction(pMnode, pTrans, pNew, pVgroup, pArray) != 0) {
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
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_ALTER_DB, pReq);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to alter db:%s", pTrans->id, pOld->name);

  mndTransSetDbInfo(pTrans, pOld);
  if (mndSetAlterDbRedoLogs(pMnode, pTrans, pOld, pNew) != 0) goto _OVER;
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
  SUserObj   *pUser = NULL;
  SAlterDbReq alterReq = {0};
  SDbObj      dbObj = {0};

  if (tDeserializeSAlterDbReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("db:%s, start to alter", alterReq.db);

  pDb = mndAcquireDb(pMnode, alterReq.db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckAlterDropCompactDbAuth(pUser, pDb) != 0) {
    goto _OVER;
  }

  memcpy(&dbObj, pDb, sizeof(SDbObj));
  if (dbObj.cfg.pRetensions != NULL) {
    dbObj.cfg.pRetensions = taosArrayDup(pDb->cfg.pRetensions);
    if (dbObj.cfg.pRetensions == NULL) goto _OVER;
  }

  code = mndSetDbCfgFromAlterDbReq(&dbObj, &alterReq);
  if (code != 0) goto _OVER;

  code = mndCheckDbCfg(pMnode, &dbObj.cfg);
  if (code != 0) goto _OVER;

  dbObj.cfgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  code = mndAlterDb(pMnode, pReq, pDb, &dbObj);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to alter since %s", alterReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  taosArrayDestroy(dbObj.cfg.pRetensions);

  return code;
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

  pDb = mndAcquireDb(pMnode, cfgReq.db);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  cfgRsp.numOfVgroups = pDb->cfg.numOfVgroups;
  cfgRsp.numOfStables = pDb->cfg.numOfStables;
  cfgRsp.buffer = pDb->cfg.buffer;
  cfgRsp.pageSize = pDb->cfg.pageSize;
  cfgRsp.pages = pDb->cfg.pages;
  cfgRsp.daysPerFile = pDb->cfg.daysPerFile;
  cfgRsp.daysToKeep0 = pDb->cfg.daysToKeep0;
  cfgRsp.daysToKeep1 = pDb->cfg.daysToKeep1;
  cfgRsp.daysToKeep2 = pDb->cfg.daysToKeep2;
  cfgRsp.minRows = pDb->cfg.minRows;
  cfgRsp.maxRows = pDb->cfg.maxRows;
  cfgRsp.fsyncPeriod = pDb->cfg.fsyncPeriod;
  cfgRsp.walLevel = pDb->cfg.walLevel;
  cfgRsp.precision = pDb->cfg.precision;
  cfgRsp.compression = pDb->cfg.compression;
  cfgRsp.replications = pDb->cfg.replications;
  cfgRsp.strict = pDb->cfg.strict;
  cfgRsp.cacheLastRow = pDb->cfg.cacheLastRow;
  cfgRsp.numOfRetensions = pDb->cfg.numOfRetensions;
  cfgRsp.pRetensions = pDb->cfg.pRetensions;

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

  if (code != 0) {
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
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_DB, pReq);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to drop db:%s", pTrans->id, pDb->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetDropDbRedoLogs(pMnode, pTrans, pDb) != 0) goto _OVER;
  if (mndSetDropDbCommitLogs(pMnode, pTrans, pDb) != 0) goto _OVER;
  /*if (mndDropOffsetByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  /*if (mndDropSubByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  /*if (mndDropTopicByDB(pMnode, pTrans, pDb) != 0) goto _OVER;*/
  if (mndSetDropDbRedoActions(pMnode, pTrans, pDb) != 0) goto _OVER;

  SUserObj *pUser = mndAcquireUser(pMnode, pDb->createUser);
  if (pUser != NULL) {
    pUser->authVersion++;
    SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
    if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
      mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
      goto _OVER;
    }
    sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  }

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
  SUserObj  *pUser = NULL;
  SDropDbReq dropReq = {0};

  if (tDeserializeSDropDbReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("db:%s, start to drop", dropReq.db);

  pDb = mndAcquireDb(pMnode, dropReq.db);
  if (pDb == NULL) {
    if (dropReq.ignoreNotExists) {
      code = mndBuildDropDbRsp(pDb, &pReq->info.rspLen, &pReq->info.rsp, true);
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto _OVER;
    }
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckAlterDropCompactDbAuth(pUser, pDb) != 0) {
    goto _OVER;
  }

  code = mndDropDb(pMnode, pReq, pDb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop since %s", dropReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

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

    if (pVgroup->dbUid == pDb->uid) {
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

  int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

  if (pReq == NULL || pReq->vgVersion < pDb->vgVersion || pReq->dbId != pDb->uid || numOfTable != pReq->numOfTable) {
    mndBuildDBVgroupInfo(pDb, pMnode, pRsp->pVgroupInfos);
  }

  memcpy(pRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
  pRsp->uid = pDb->uid;
  pRsp->vgVersion = pDb->vgVersion;
  pRsp->vgNum = taosArrayGetSize(pRsp->pVgroupInfos);
  pRsp->hashMethod = pDb->cfg.hashMethod;
  return 0;
}

static int32_t mndProcessUseDbReq(SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SUserObj *pUser = NULL;
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
      if (usedbRsp.pVgroupInfos == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        goto _OVER;
      }

      mndBuildDBVgroupInfo(NULL, pMnode, usedbRsp.pVgroupInfos);
      usedbRsp.vgVersion = vgVersion++;
    } else {
      usedbRsp.vgVersion = usedbReq.vgVersion;
    }
    usedbRsp.vgNum = taosArrayGetSize(usedbRsp.pVgroupInfos);
    code = 0;

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
      pUser = mndAcquireUser(pMnode, pReq->conn.user);
      if (pUser == NULL) {
        goto _OVER;
      }

      if (mndCheckUseDbAuth(pUser, pDb) != 0) {
        goto _OVER;
      }

      if (mndExtractDbInfo(pMnode, pDb, &usedbRsp, &usedbReq) < 0) {
        goto _OVER;
      }

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

    int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

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
    usedbRsp.hashMethod = pDb->cfg.hashMethod;

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

static int32_t mndProcessCompactDbReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDbObj       *pDb = NULL;
  SUserObj     *pUser = NULL;
  SCompactDbReq compactReq = {0};

  if (tDeserializeSCompactDbReq(pReq->pCont, pReq->contLen, &compactReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("db:%s, start to sync", compactReq.db);

  pDb = mndAcquireDb(pMnode, compactReq.db);
  if (pDb == NULL) {
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckAlterDropCompactDbAuth(pUser, pDb) != 0) {
    goto _OVER;
  }

  // code = mndCompactDb();

_OVER:
  if (code != 0) {
    mError("db:%s, failed to process compact db req since %s", compactReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

const char *mndGetDbStr(const char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;
  if (pos == NULL) return src;
  return pos;
}

static void dumpDbInfoData(SSDataBlock *pBlock, SDbObj *pDb, SShowObj *pShow, int32_t rows, int64_t numOfTables,
                           bool sysDb) {
  int32_t cols = 0;

  int32_t     bytes = pShow->pMeta->pSchemas[cols].bytes;
  char       *buf = taosMemoryMalloc(bytes);
  const char *name = mndGetDbStr(pDb->name);
  if (name != NULL) {
    STR_WITH_MAXSIZE_TO_VARSTR(buf, name, bytes);
  } else {
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "NULL", bytes);
  }

  char *status = "ready";
  char  statusB[24] = {0};
  STR_WITH_SIZE_TO_VARSTR(statusB, status, strlen(status));

  if (sysDb) {
    for (int32_t i = 0; i < pShow->numOfColumns; ++i) {
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      if (i == 0) {
        colDataAppend(pColInfo, rows, buf, false);
      } else if (i == 3) {
        colDataAppend(pColInfo, rows, (const char *)&numOfTables, false);
      } else if (i == 20) {
        colDataAppend(pColInfo, rows, statusB, false);
      } else {
        colDataAppendNULL(pColInfo, rows);
      }
    }
  } else {
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, buf, false);
    taosMemoryFree(buf);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.numOfVgroups, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&numOfTables, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.replications, false);

    const char *src = pDb->cfg.strict ? "strict" : "nostrict";
    char        strict[24] = {0};
    STR_WITH_SIZE_TO_VARSTR(strict, src, strlen(src));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)strict, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.daysPerFile, false);

    char    tmp[128] = {0};
    int32_t len = 0;
    if (pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep1 || pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep2) {
      len = sprintf(&tmp[VARSTR_HEADER_SIZE], "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2,
                    pDb->cfg.daysToKeep0);
    } else {
      len = sprintf(&tmp[VARSTR_HEADER_SIZE], "%d,%d,%d", pDb->cfg.daysToKeep0, pDb->cfg.daysToKeep1,
                    pDb->cfg.daysToKeep2);
    }

    varDataSetLen(tmp, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)tmp, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.buffer, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.pageSize, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.pages, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.minRows, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.maxRows, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.walLevel, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.fsyncPeriod, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.compression, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.cacheLastRow, false);

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

    char t[10] = {0};
    STR_WITH_SIZE_TO_VARSTR(t, prec, 2);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)t, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, rows, (const char *)&pDb->cfg.numOfStables, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppend(pColInfo, rows, (const char *)statusB, false);
  }
}

static void setInformationSchemaDbCfg(SDbObj *pDbObj) {
  ASSERT(pDbObj != NULL);
  strncpy(pDbObj->name, TSDB_INFORMATION_SCHEMA_DB, tListLen(pDbObj->name));

  pDbObj->createdTime = 0;
  pDbObj->cfg.numOfVgroups = 0;
  pDbObj->cfg.strict = 1;
  pDbObj->cfg.replications = 1;
  pDbObj->cfg.precision = TSDB_TIME_PRECISION_MILLI;
}

static void setPerfSchemaDbCfg(SDbObj *pDbObj) {
  ASSERT(pDbObj != NULL);
  strncpy(pDbObj->name, TSDB_PERFORMANCE_SCHEMA_DB, tListLen(pDbObj->name));

  pDbObj->createdTime = 0;
  pDbObj->cfg.numOfVgroups = 0;
  pDbObj->cfg.strict = 1;
  pDbObj->cfg.replications = 1;
  pDbObj->cfg.precision = TSDB_TIME_PRECISION_MILLI;
}

static bool mndGetTablesOfDbFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int32_t *numOfTables = p1;
  *numOfTables += pVgroup->numOfTables;
  return true;
}

static int32_t mndRetrieveDbs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SDbObj *pDb = NULL;

  // Append the information_schema database into the result.
  if (!pShow->sysDbRsp) {
    SDbObj infoschemaDb = {0};
    setInformationSchemaDbCfg(&infoschemaDb);
    dumpDbInfoData(pBlock, &infoschemaDb, pShow, numOfRows, 14, true);

    numOfRows += 1;

    SDbObj perfschemaDb = {0};
    setPerfSchemaDbCfg(&perfschemaDb);
    dumpDbInfoData(pBlock, &perfschemaDb, pShow, numOfRows, 3, true);

    numOfRows += 1;
    pShow->sysDbRsp = true;
  }

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_DB, pShow->pIter, (void **)&pDb);
    if (pShow->pIter == NULL) {
      break;
    }

    int32_t numOfTables = 0;
    sdbTraverse(pSdb, SDB_VGROUP, mndGetTablesOfDbFp, &numOfTables, NULL, NULL);

    dumpDbInfoData(pBlock, pDb, pShow, numOfRows, numOfTables, false);
    numOfRows++;
    sdbRelease(pSdb, pDb);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextDb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
