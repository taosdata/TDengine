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
#include "audit.h"
#include "command.h"
#include "mndArbGroup.h"
#include "mndCluster.h"
#include "mndConfig.h"
#include "mndDb.h"
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
#include "thttp.h"
#include "tjson.h"

#define MOUNT_VER_NUMBER   1
#define MOUNT_RESERVE_SIZE 128

static SSdbRaw *mndMountActionEncode(SMountObj *pDb);
static SSdbRow *mndMountActionDecode(SSdbRaw *pRaw);
static int32_t  mndMountActionInsert(SSdb *pSdb, SMountObj *pDb);
static int32_t  mndMountActionDelete(SSdb *pSdb, SMountObj *pDb);
static int32_t  mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew);
static int32_t  mndNewMountActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw);

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq);
static int32_t mndProcessDropMountReq(SRpcMsg *pReq);
static int32_t mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity);
static void    mndCancelGetNextMount(SMnode *pMnode, void *pIter);

int32_t mndInitMount(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_MOUNT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndMountActionEncode,
      .decodeFp = (SdbDecodeFp)mndMountActionDecode,
      .insertFp = (SdbInsertFp)mndMountActionInsert,
      .updateFp = (SdbUpdateFp)mndMountActionUpdate,
      .deleteFp = (SdbDeleteFp)mndMountActionDelete,
      .validateFp = (SdbValidateFp)mndNewMountActionValidate,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_MOUNT, mndProcessCreateMountReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MOUNT, mndProcessDropMountReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndRetrieveMounts);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndCancelGetNextMount);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMount(SMnode *pMnode) {}

SSdbRaw *mndMountActionEncode(SMountObj *pDb) {
#if 0
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SDbObj) + pDb->cfg.numOfRetensions * sizeof(SRetention) + DB_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DB, DB_VER_NUMBER, size);
  if (pRaw == NULL) goto _exit;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, _exit)
  SDB_SET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, _exit)
  SDB_SET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->createdTime, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->updateTime, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->uid, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfgVersion, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->vgVersion, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfVgroups, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfStables, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.buffer, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.pageSize, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.pages, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.cacheLastSize, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysPerFile, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep0, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep1, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.daysToKeep2, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.minRows, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.maxRows, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walFsyncPeriod, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.walLevel, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.precision, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compression, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.replications, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.strict, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.cacheLast, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.hashMethod, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.numOfRetensions, _exit)
  for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pDb->cfg.pRetensions, i);
    SDB_SET_INT64(pRaw, dataPos, pRetension->freq, _exit)
    SDB_SET_INT64(pRaw, dataPos, pRetension->keep, _exit)
    SDB_SET_INT8(pRaw, dataPos, pRetension->freqUnit, _exit)
    SDB_SET_INT8(pRaw, dataPos, pRetension->keepUnit, _exit)
  }
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.schemaless, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walRetentionPeriod, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->cfg.walRetentionSize, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.walRollPeriod, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->cfg.walSegmentSize, _exit)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.sstTrigger, _exit)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.hashPrefix, _exit)
  SDB_SET_INT16(pRaw, dataPos, pDb->cfg.hashSuffix, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.tsdbPageSize, _exit)
  SDB_SET_INT64(pRaw, dataPos, pDb->compactStartTime, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.keepTimeOffset, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.s3ChunkSize, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.s3KeepLocal, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.s3Compact, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.withArbitrator, _exit)
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.encryptAlgorithm, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->tsmaVersion, _exit);
  SDB_SET_INT8(pRaw, dataPos, pDb->cfg.compactTimeOffset, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.compactStartTime, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.compactEndTime, _exit)
  SDB_SET_INT32(pRaw, dataPos, pDb->cfg.compactInterval, _exit)

  SDB_SET_RESERVE(pRaw, dataPos, DB_RESERVE_SIZE, _exit)
  SDB_SET_DATALEN(pRaw, dataPos, _exit)

  terrno = 0;

_exit:
  if (terrno != 0) {
    mError("db:%s, failed to encode to raw:%p since %s", pDb->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("db:%s, encode to raw:%p, row:%p", pDb->name, pRaw, pDb);
  return pRaw;
#endif
  return NULL;
}

static SSdbRow *mndMountActionDecode(SSdbRaw *pRaw) {
#if 0
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SDbObj  *pDb = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _exit;

  if (sver != DB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _exit;
  }

  pRow = sdbAllocRow(sizeof(SDbObj));
  if (pRow == NULL) goto _exit;

  pDb = sdbGetRowObj(pRow);
  if (pDb == NULL) goto _exit;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pDb->name, TSDB_DB_FNAME_LEN, _exit)
  SDB_GET_BINARY(pRaw, dataPos, pDb->acct, TSDB_USER_LEN, _exit)
  SDB_GET_BINARY(pRaw, dataPos, pDb->createUser, TSDB_USER_LEN, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->createdTime, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->updateTime, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->uid, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfgVersion, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->vgVersion, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfVgroups, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfStables, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.buffer, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.pageSize, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.pages, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.cacheLastSize, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysPerFile, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep0, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep1, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.daysToKeep2, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.minRows, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.maxRows, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walFsyncPeriod, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.walLevel, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.precision, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.compression, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.replications, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.strict, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.cacheLast, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.hashMethod, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.numOfRetensions, _exit)
  if (pDb->cfg.numOfRetensions > 0) {
    pDb->cfg.pRetensions = taosArrayInit(pDb->cfg.numOfRetensions, sizeof(SRetention));
    if (pDb->cfg.pRetensions == NULL) goto _exit;
    for (int32_t i = 0; i < pDb->cfg.numOfRetensions; ++i) {
      SRetention retention = {0};
      SDB_GET_INT64(pRaw, dataPos, &retention.freq, _exit)
      SDB_GET_INT64(pRaw, dataPos, &retention.keep, _exit)
      SDB_GET_INT8(pRaw, dataPos, &retention.freqUnit, _exit)
      SDB_GET_INT8(pRaw, dataPos, &retention.keepUnit, _exit)
      if (taosArrayPush(pDb->cfg.pRetensions, &retention) == NULL) {
        goto _exit;
      }
    }
  }
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.schemaless, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walRetentionPeriod, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->cfg.walRetentionSize, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.walRollPeriod, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->cfg.walSegmentSize, _exit)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.sstTrigger, _exit)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.hashPrefix, _exit)
  SDB_GET_INT16(pRaw, dataPos, &pDb->cfg.hashSuffix, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.tsdbPageSize, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pDb->compactStartTime, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.keepTimeOffset, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.s3ChunkSize, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.s3KeepLocal, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.s3Compact, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.withArbitrator, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.encryptAlgorithm, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->tsmaVersion, _exit);
  SDB_GET_INT8(pRaw, dataPos, &pDb->cfg.compactTimeOffset, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.compactStartTime, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.compactEndTime, _exit)
  SDB_GET_INT32(pRaw, dataPos, &pDb->cfg.compactInterval, _exit)

  SDB_GET_RESERVE(pRaw, dataPos, DB_RESERVE_SIZE, _exit)
  taosInitRWLatch(&pDb->lock);

  if (pDb->cfg.s3ChunkSize == 0) {
    pDb->cfg.s3ChunkSize = TSDB_DEFAULT_S3_CHUNK_SIZE;

    mInfo("db:%s, s3ChunkSize set from %d to default %d", pDb->name, pDb->cfg.s3ChunkSize, TSDB_DEFAULT_S3_CHUNK_SIZE);
  }

  if (pDb->cfg.s3KeepLocal == 0) {
    pDb->cfg.s3KeepLocal = TSDB_DEFAULT_S3_KEEP_LOCAL;

    mInfo("db:%s, s3KeepLocal set from %d to default %d", pDb->name, pDb->cfg.s3KeepLocal, TSDB_DEFAULT_S3_KEEP_LOCAL);
  }

  if (pDb->cfg.tsdbPageSize != TSDB_MIN_TSDB_PAGESIZE) {
    mInfo("db:%s, tsdbPageSize set from %d to default %d", pDb->name, pDb->cfg.tsdbPageSize,
          TSDB_DEFAULT_TSDB_PAGESIZE);
  }

  if (pDb->cfg.sstTrigger != TSDB_MIN_STT_TRIGGER) {
    mInfo("db:%s, sstTrigger set from %d to default %d", pDb->name, pDb->cfg.sstTrigger, TSDB_DEFAULT_SST_TRIGGER);
  }

  terrno = 0;

_exit:
  if (terrno != 0) {
    mError("db:%s, failed to decode from raw:%p since %s", pDb == NULL ? "null" : pDb->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("db:%s, decode from raw:%p, row:%p", pDb->name, pRaw, pDb);
  return pRow;
#endif
  return NULL;
}

static int32_t mndNewMountActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw) {
#if 0
  SSdb    *pSdb = pMnode->pSdb;
  SSdbRow *pRow = NULL;
  SDbObj  *pNewDb = NULL;
  int      code = -1;

  pRow = mndDbActionDecode(pRaw);
  if (pRow == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }
  pNewDb = sdbGetRowObj(pRow);
  if (pNewDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  SDbObj *pOldDb = sdbAcquire(pMnode->pSdb, SDB_DB, pNewDb->name);
  if (pOldDb != NULL) {
    mError("trans:%d, db name already in use. name: %s", pTrans->id, pNewDb->name);
    sdbRelease(pMnode->pSdb, pOldDb);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  code = 0;
_exit:
  if (pNewDb) mndDbActionDelete(pSdb, pNewDb);
  taosMemoryFreeClear(pRow);
  return code;
#endif
  return 0;
}

static int32_t mndMountActionInsert(SSdb *pSdb, SMountObj *pObj) {
  mTrace("mount:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndMountActionDelete(SSdb *pSdb, SMountObj *pObj) {
  mTrace("mount:%s, perform delete action, row:%p", pObj->name, pObj);
  mndMountFreeObj(pObj);
  return 0;
}

static int32_t mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew) {
  mTrace("mount:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  ASSERT(0); // TODO
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SMountObj *mndAcquireMount(SMnode *pMnode, const char *mountName) {
  SSdb      *pSdb = pMnode->pSdb;
  SMountObj *pObj = sdbAcquire(pSdb, SDB_MOUNT, mountName);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("mount:%s, failed to acquire mount since %s", mountName, terrstr());
    }
  }
  return pObj;
}

void mndReleaseMount(SMnode *pMnode, SMountObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

bool mndMountIsExist(SMnode *pMnode, const char *mountName) {
  SMountObj *pObj = mndAcquireMount(pMnode, mountName);
  if (pObj == NULL) {
    return false;
  }
  mndReleaseMount(pMnode, pObj);
  return true;
}
#if 0
static int32_t mndCheckDbName(const char *dbName, SUserObj *pUser) {
  char *pos = strstr(dbName, TS_PATH_DELIMITER);
  if (pos == NULL) {
    return TSDB_CODE_MND_INVALID_DB;
  }

  int32_t acctId;
  int32_t code = taosStr2int32(dbName, &acctId);
  if (code != 0) {
    return code;
  }

  if (acctId != pUser->acctId) {
    return TSDB_CODE_MND_INVALID_DB_ACCT;
  }

  return 0;
}

static int32_t mndCheckDbCfg(SMnode *pMnode, SDbCfg *pCfg) {
  int32_t code = TSDB_CODE_MND_INVALID_DB_OPTION;

  if (pCfg->numOfVgroups < TSDB_MIN_VNODES_PER_DB || pCfg->numOfVgroups > TSDB_MAX_VNODES_PER_DB) return code;
  if (pCfg->numOfStables < TSDB_DB_STREAM_MODE_OFF || pCfg->numOfStables > TSDB_DB_STREAM_MODE_ON) return code;
  if (pCfg->buffer < TSDB_MIN_BUFFER_PER_VNODE || pCfg->buffer > TSDB_MAX_BUFFER_PER_VNODE) return code;
  if (pCfg->pageSize < TSDB_MIN_PAGESIZE_PER_VNODE || pCfg->pageSize > TSDB_MAX_PAGESIZE_PER_VNODE) return code;
  if (pCfg->pages < TSDB_MIN_PAGES_PER_VNODE || pCfg->pages > TSDB_MAX_PAGES_PER_VNODE) return code;
  if (pCfg->cacheLastSize < TSDB_MIN_DB_CACHE_SIZE || pCfg->cacheLastSize > TSDB_MAX_DB_CACHE_SIZE) return code;
  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return code;
  if (pCfg->daysToKeep0 < TSDB_MIN_KEEP || pCfg->daysToKeep0 > TSDB_MAX_KEEP) return code;
  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > TSDB_MAX_KEEP) return code;
  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > TSDB_MAX_KEEP) return code;
  if (pCfg->daysToKeep0 < pCfg->daysPerFile) return code;
  if (pCfg->daysToKeep0 > pCfg->daysToKeep1) return code;
  if (pCfg->daysToKeep1 > pCfg->daysToKeep2) return code;
  if (pCfg->keepTimeOffset < TSDB_MIN_KEEP_TIME_OFFSET || pCfg->keepTimeOffset > TSDB_MAX_KEEP_TIME_OFFSET) return code;
  if (pCfg->minRows < TSDB_MIN_MINROWS_FBLOCK || pCfg->minRows > TSDB_MAX_MINROWS_FBLOCK) return code;
  if (pCfg->maxRows < TSDB_MIN_MAXROWS_FBLOCK || pCfg->maxRows > TSDB_MAX_MAXROWS_FBLOCK) return code;
  if (pCfg->minRows > pCfg->maxRows) return code;
  if (pCfg->walFsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->walFsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return code;
  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) return code;
  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) return code;
  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) return code;
  if (pCfg->replications < TSDB_MIN_DB_REPLICA || pCfg->replications > TSDB_MAX_DB_REPLICA) return code;
#ifdef TD_ENTERPRISE
  if ((pCfg->replications == 2) ^ (pCfg->withArbitrator == TSDB_MAX_DB_WITH_ARBITRATOR)) return code;
  if (pCfg->encryptAlgorithm < TSDB_MIN_ENCRYPT_ALGO || pCfg->encryptAlgorithm > TSDB_MAX_ENCRYPT_ALGO) return code;
#else
  if (pCfg->replications != 1 && pCfg->replications != 3) return code;
  if (pCfg->encryptAlgorithm != TSDB_DEFAULT_ENCRYPT_ALGO) return code;
#endif

  if (pCfg->strict < TSDB_DB_STRICT_OFF || pCfg->strict > TSDB_DB_STRICT_ON) return code;
  if (pCfg->schemaless < TSDB_DB_SCHEMALESS_OFF || pCfg->schemaless > TSDB_DB_SCHEMALESS_ON) return code;
  if (pCfg->cacheLast < TSDB_CACHE_MODEL_NONE || pCfg->cacheLast > TSDB_CACHE_MODEL_BOTH) return code;
  if (pCfg->hashMethod != 1) return code;
  if (pCfg->replications > mndGetDnodeSize(pMnode)) {
    code = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    TAOS_RETURN(code);
  }
  if (pCfg->walRetentionPeriod < TSDB_DB_MIN_WAL_RETENTION_PERIOD) return code;
  if (pCfg->walRetentionSize < TSDB_DB_MIN_WAL_RETENTION_SIZE) return code;
  if (pCfg->walRollPeriod < TSDB_DB_MIN_WAL_ROLL_PERIOD) return code;
  if (pCfg->walSegmentSize < TSDB_DB_MIN_WAL_SEGMENT_SIZE) return code;
  if (pCfg->sstTrigger < TSDB_MIN_STT_TRIGGER || pCfg->sstTrigger > TSDB_MAX_STT_TRIGGER) return code;
  if (pCfg->hashPrefix < TSDB_MIN_HASH_PREFIX || pCfg->hashPrefix > TSDB_MAX_HASH_PREFIX) return code;
  if (pCfg->hashSuffix < TSDB_MIN_HASH_SUFFIX || pCfg->hashSuffix > TSDB_MAX_HASH_SUFFIX) return code;
  if ((pCfg->hashSuffix * pCfg->hashPrefix) < 0) return code;
  if ((pCfg->hashPrefix + pCfg->hashSuffix) >= (TSDB_TABLE_NAME_LEN - 1)) return code;
  if (pCfg->tsdbPageSize < TSDB_MIN_TSDB_PAGESIZE || pCfg->tsdbPageSize > TSDB_MAX_TSDB_PAGESIZE) return code;
  if (taosArrayGetSize(pCfg->pRetensions) != pCfg->numOfRetensions) return code;

  if (pCfg->s3ChunkSize < TSDB_MIN_S3_CHUNK_SIZE || pCfg->s3ChunkSize > TSDB_MAX_S3_CHUNK_SIZE) return code;
  if (pCfg->s3KeepLocal < TSDB_MIN_S3_KEEP_LOCAL || pCfg->s3KeepLocal > TSDB_MAX_S3_KEEP_LOCAL) return code;
  if (pCfg->s3Compact < TSDB_MIN_S3_COMPACT || pCfg->s3Compact > TSDB_MAX_S3_COMPACT) return code;

  if (pCfg->compactInterval != 0 &&
      (pCfg->compactInterval < TSDB_MIN_COMPACT_INTERVAL || pCfg->compactInterval > pCfg->daysToKeep2))
    return code;
  if (pCfg->compactStartTime != 0 &&
      (pCfg->compactStartTime < -pCfg->daysToKeep2 || pCfg->compactStartTime > -pCfg->daysPerFile))
    return code;
  if (pCfg->compactEndTime != 0 &&
      (pCfg->compactEndTime < -pCfg->daysToKeep2 || pCfg->compactEndTime > -pCfg->daysPerFile))
    return code;
  if (pCfg->compactStartTime != 0 && pCfg->compactEndTime != 0 && pCfg->compactStartTime >= pCfg->compactEndTime)
    return code;
  if (pCfg->compactTimeOffset < TSDB_MIN_COMPACT_TIME_OFFSET || pCfg->compactTimeOffset > TSDB_MAX_COMPACT_TIME_OFFSET)
    return code;

  code = 0;
  TAOS_RETURN(code);
}

static int32_t mndCheckInChangeDbCfg(SMnode *pMnode, SDbCfg *pOldCfg, SDbCfg *pNewCfg) {
  int32_t code = TSDB_CODE_MND_INVALID_DB_OPTION;
  if (pNewCfg->buffer < TSDB_MIN_BUFFER_PER_VNODE || pNewCfg->buffer > TSDB_MAX_BUFFER_PER_VNODE) return code;
  if (pNewCfg->pages < TSDB_MIN_PAGES_PER_VNODE || pNewCfg->pages > TSDB_MAX_PAGES_PER_VNODE) return code;
  if (pNewCfg->pageSize < TSDB_MIN_PAGESIZE_PER_VNODE || pNewCfg->pageSize > TSDB_MAX_PAGESIZE_PER_VNODE) return code;
  if (pNewCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pNewCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) return code;
  if (pNewCfg->daysToKeep0 < TSDB_MIN_KEEP || pNewCfg->daysToKeep0 > TSDB_MAX_KEEP) return code;
  if (pNewCfg->daysToKeep1 < TSDB_MIN_KEEP || pNewCfg->daysToKeep1 > TSDB_MAX_KEEP) return code;
  if (pNewCfg->daysToKeep2 < TSDB_MIN_KEEP || pNewCfg->daysToKeep2 > TSDB_MAX_KEEP) return code;
  if (pNewCfg->daysToKeep0 < pNewCfg->daysPerFile) return code;
  if (pNewCfg->daysToKeep0 > pNewCfg->daysToKeep1) return code;
  if (pNewCfg->daysToKeep1 > pNewCfg->daysToKeep2) return code;
  if (pNewCfg->keepTimeOffset < TSDB_MIN_KEEP_TIME_OFFSET || pNewCfg->keepTimeOffset > TSDB_MAX_KEEP_TIME_OFFSET)
    return code;
  if (pNewCfg->walFsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pNewCfg->walFsyncPeriod > TSDB_MAX_FSYNC_PERIOD) return code;
  if (pNewCfg->walLevel < TSDB_MIN_WAL_LEVEL || pNewCfg->walLevel > TSDB_MAX_WAL_LEVEL) return code;
  if (pNewCfg->cacheLast < TSDB_CACHE_MODEL_NONE || pNewCfg->cacheLast > TSDB_CACHE_MODEL_BOTH) return code;
  if (pNewCfg->cacheLastSize < TSDB_MIN_DB_CACHE_SIZE || pNewCfg->cacheLastSize > TSDB_MAX_DB_CACHE_SIZE) return code;
  if (pNewCfg->replications < TSDB_MIN_DB_REPLICA || pNewCfg->replications > TSDB_MAX_DB_REPLICA) return code;
#ifdef TD_ENTERPRISE
  if ((pNewCfg->replications == 2) ^ (pNewCfg->withArbitrator == TSDB_MAX_DB_WITH_ARBITRATOR)) return code;
  if (pNewCfg->replications == 2 && pNewCfg->withArbitrator == TSDB_MAX_DB_WITH_ARBITRATOR) {
    if (pOldCfg->replications != 1 && pOldCfg->replications != 2) {
      terrno = TSDB_CODE_OPS_NOT_SUPPORT;
      return code;
    }
  }
  if (pNewCfg->replications != 2 && pOldCfg->replications == 2) {
    terrno = TSDB_CODE_OPS_NOT_SUPPORT;
    return code;
  }
#else
  if (pNewCfg->replications != 1 && pNewCfg->replications != 3) return code;
#endif

  if (pNewCfg->walLevel == 0 && pOldCfg->replications > 1) {
    terrno = TSDB_CODE_MND_INVALID_WAL_LEVEL;
    return code;
  }
  if (pNewCfg->replications > 1 && pOldCfg->walLevel == 0) {
    terrno = TSDB_CODE_MND_INVALID_WAL_LEVEL;
    return code;
  }

  if (pNewCfg->sstTrigger != pOldCfg->sstTrigger &&
      (pNewCfg->sstTrigger < TSDB_MIN_STT_TRIGGER || pNewCfg->sstTrigger > TSDB_MAX_STT_TRIGGER))
    return code;
  if (pNewCfg->minRows < TSDB_MIN_MINROWS_FBLOCK || pNewCfg->minRows > TSDB_MAX_MINROWS_FBLOCK) return code;
  if (pNewCfg->maxRows < TSDB_MIN_MAXROWS_FBLOCK || pNewCfg->maxRows > TSDB_MAX_MAXROWS_FBLOCK) return code;
  if (pNewCfg->minRows > pNewCfg->maxRows) return code;
  if (pNewCfg->walRetentionPeriod < TSDB_DB_MIN_WAL_RETENTION_PERIOD) return code;
  if (pNewCfg->walRetentionSize < TSDB_DB_MIN_WAL_RETENTION_SIZE) return code;
  if (pNewCfg->strict < TSDB_DB_STRICT_OFF || pNewCfg->strict > TSDB_DB_STRICT_ON) return code;
  if (pNewCfg->replications > mndGetDnodeSize(pMnode)) {
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return code;
  }
  if (pNewCfg->s3ChunkSize < TSDB_MIN_S3_CHUNK_SIZE || pNewCfg->s3ChunkSize > TSDB_MAX_S3_CHUNK_SIZE) return code;
  if (pNewCfg->s3KeepLocal < TSDB_MIN_S3_KEEP_LOCAL || pNewCfg->s3KeepLocal > TSDB_MAX_S3_KEEP_LOCAL) return code;
  if (pNewCfg->s3Compact < TSDB_MIN_S3_COMPACT || pNewCfg->s3Compact > TSDB_MAX_S3_COMPACT) return code;

  if (pNewCfg->compactInterval != 0 &&
      (pNewCfg->compactInterval < TSDB_MIN_COMPACT_INTERVAL || pNewCfg->compactInterval > pNewCfg->daysToKeep2))
    return code;
  if (pNewCfg->compactStartTime != 0 &&
      (pNewCfg->compactStartTime < -pNewCfg->daysToKeep2 || pNewCfg->compactStartTime > -pNewCfg->daysPerFile))
    return code;
  if (pNewCfg->compactEndTime != 0 &&
      (pNewCfg->compactEndTime < -pNewCfg->daysToKeep2 || pNewCfg->compactEndTime > -pNewCfg->daysPerFile))
    return code;
  if (pNewCfg->compactStartTime != 0 && pNewCfg->compactEndTime != 0 &&
      pNewCfg->compactStartTime >= pNewCfg->compactEndTime)
    return code;
  if (pNewCfg->compactTimeOffset < TSDB_MIN_COMPACT_TIME_OFFSET ||
      pNewCfg->compactTimeOffset > TSDB_MAX_COMPACT_TIME_OFFSET)
    return code;

  code = 0;
  TAOS_RETURN(code);
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
  if (pCfg->s3ChunkSize <= 0) pCfg->s3ChunkSize = TSDB_DEFAULT_S3_CHUNK_SIZE;
  if (pCfg->s3KeepLocal <= 0) pCfg->s3KeepLocal = TSDB_DEFAULT_S3_KEEP_LOCAL;
  if (pCfg->s3Compact < 0) pCfg->s3Compact = TSDB_DEFAULT_S3_COMPACT;
  if (pCfg->withArbitrator < 0) pCfg->withArbitrator = TSDB_DEFAULT_DB_WITH_ARBITRATOR;
  if (pCfg->encryptAlgorithm < 0) pCfg->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
}
#endif


static int32_t mndSetCreateMountPrepareAction(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  SSdbRaw *pDbRaw = mndMountActionEncode(pObj);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}
#if 0
static int32_t mndSetNewVgPrepareActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    if (mndAddNewVgPrepareAction(pMnode, pTrans, (pVgroups + v)) != 0) return -1;
  }
  return 0;
}

static int32_t mndSetCreateDbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t  code = 0;
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pDbRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_UPDATE));

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pVgRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pVgRaw, SDB_STATUS_UPDATE));
  }

  if (pDb->cfg.withArbitrator) {
    for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
      SVgObj   *pVgObj = pVgroups + v;
      SArbGroup arbGroup = {0};
      TAOS_CHECK_RETURN(mndArbGroupInitFromVgObj(pVgObj, &arbGroup));
      TAOS_CHECK_RETURN(mndSetCreateArbGroupRedoLogs(pTrans, &arbGroup));
    }
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateDbUndoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t  code = 0;
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pDbRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_DROPPED));

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pVgRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED));
  }

  if (pDb->cfg.withArbitrator) {
    for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
      SVgObj   *pVgObj = pVgroups + v;
      SArbGroup arbGroup = {0};
      TAOS_CHECK_RETURN(mndArbGroupInitFromVgObj(pVgObj, &arbGroup));
      TAOS_CHECK_RETURN(mndSetCreateArbGroupUndoLogs(pTrans, &arbGroup));
    }
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups,
                                        SUserObj *pUserDuped) {
  int32_t  code = 0;
  SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
  if (pDbRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pDbRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_READY));

  for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroups + v);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pVgRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pVgRaw, SDB_STATUS_READY));
  }

  if (pDb->cfg.withArbitrator) {
    for (int32_t v = 0; v < pDb->cfg.numOfVgroups; ++v) {
      SVgObj   *pVgObj = pVgroups + v;
      SArbGroup arbGroup = {0};
      TAOS_CHECK_RETURN(mndArbGroupInitFromVgObj(pVgObj, &arbGroup));
      TAOS_CHECK_RETURN(mndSetCreateArbGroupCommitLogs(pTrans, &arbGroup));
    }
  }

  if (pUserDuped) {
    SSdbRaw *pUserRaw = mndUserActionEncode(pUserDuped);
    if (pUserRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pUserRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pUserRaw, SDB_STATUS_READY));
  }

  TAOS_RETURN(code);
}
#endif
static int32_t mndSetCreateMountRedoActions(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t code = 0;
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid));
    }
  }

  TAOS_RETURN(code);
}
#if 0
static int32_t mndSetCreateDbUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroups) {
  int32_t code = 0;
  for (int32_t vg = 0; vg < pDb->cfg.numOfVgroups; ++vg) {
    SVgObj *pVgroup = pVgroups + vg;

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
      TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, false));
    }
  }

  TAOS_RETURN(code);
}
#endif
static int32_t mndCreateMount(SMnode *pMnode, SRpcMsg *pReq, SCreateMountReq *pCreate, SUserObj *pUser) {
  int32_t   code = 0, lino = 0;
  SUserObj  newUserObj = {0};
  SMountObj mntObj = {0};
  (void)memcpy(mntObj.name, pCreate->mountName, TSDB_MOUNT_NAME_LEN);
  (void)memcpy(mntObj.acct, pUser->acct, TSDB_USER_LEN);
  mntObj.createdTime = taosGetTimestampMs();
  mntObj.updateTime = mntObj.createdTime;
  mntObj.uid = mndGenerateUid(mntObj.name, TSDB_MOUNT_NAME_LEN);
  (void)memcpy(mntObj.createUser, pUser->user, TSDB_USER_LEN);
  // dbCfg
  // mntObj.dbCfg = pCreate->dbCfg;
  // mntObj.dbName = pCreate->dbName;
  // mntObj.dbUid = pCreate->dbUid;
  // mndSetDefaultDbCfg(&dbObj.cfg);

  // if ((code = mndCheckDbName(dbObj.name, pUser)) != 0) {
  //   mError("db:%s, failed to create, check db name failed, since %s", pCreate->db, terrstr());
  //   TAOS_RETURN(code);
  // }

  // if ((code = mndCheckDbCfg(pMnode, &dbObj.cfg)) != 0) {
  //   mError("db:%s, failed to create, check db cfg failed, since %s", pCreate->db, terrstr());
  //   TAOS_RETURN(code);
  // }

  // if (dbObj.cfg.hashPrefix > 0) {
  //   int32_t dbLen = strlen(dbObj.name) + 1;
  //   mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix + dbLen);
  //   dbObj.cfg.hashPrefix += dbLen;
  // } else if (dbObj.cfg.hashPrefix < 0) {
  //   int32_t dbLen = strlen(dbObj.name) + 1;
  //   mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix - dbLen);
  //   dbObj.cfg.hashPrefix -= dbLen;
  // }

  // SVgObj *pVgroups = NULL;
  // if ((code = mndAllocVgroup(pMnode, &dbObj, &pVgroups, dnodeList)) != 0) {
  //   mError("db:%s, failed to create, alloc vgroup failed, since %s", pCreate->db, terrstr());
  //   TAOS_RETURN(code);
  // }

  // add database privileges for user
  // SUserObj *pNewUserDuped = NULL;
  // if (!pUser->superUser) {
  //   TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUserObj), NULL, _exit);
  //   TAOS_CHECK_GOTO(taosHashPut(newUserObj.readDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name, TSDB_FILENAME_LEN),
  //                   NULL, _exit);
  //   TAOS_CHECK_GOTO(taosHashPut(newUserObj.writeDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name, TSDB_FILENAME_LEN),
  //                   NULL, _exit);
  //   pNewUserDuped = &newUserObj;
  // }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "create-mount");
  if (pTrans == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
    goto _exit;
  }
  // mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to create db:%s", pTrans->id, pCreate->mountName);

  mndTransSetDbName(pTrans, mntObj.name, NULL);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_CREATE_DB);
  TAOS_CHECK_EXIT(mndSetCreateMountPrepareAction(pMnode, pTrans, &mntObj));
  TAOS_CHECK_EXIT(mndSetCreateMountRedoActions(pMnode, pTrans, &mntObj));
  TAOS_CHECK_EXIT(mndSetNewVgPrepareActions(pMnode, pTrans, &mntObj, pVgroups));
  // TAOS_CHECK_EXIT(mndSetCreateDbUndoLogs(pMnode, pTrans, &mntObj, pVgroups));
  // TAOS_CHECK_EXIT(mndSetCreateDbCommitLogs(pMnode, pTrans, &mntObj, pVgroups, pNewUserDuped));
  // TAOS_CHECK_EXIT(mndSetCreateDbUndoActions(pMnode, pTrans, &mntObj, pVgroups));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  taosMemoryFree(pVgroups);
  mndUserFreeObj(&newUserObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
#if 0
static int32_t mndCheckDbEncryptKey(SMnode *pMnode, SCreateDbReq *pReq) {
  int32_t    code = 0;
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  void      *pIter = NULL;

#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  if (pReq->encryptAlgorithm == TSDB_ENCRYPT_ALGO_NONE) goto _exit;
  TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_DB_ENCRYPTION), NULL, _exit);
  if (tsEncryptionKeyStat != ENCRYPT_KEY_STAT_LOADED) {
    code = TSDB_CODE_MND_INVALID_ENCRYPT_KEY;
    mError("db:%s, failed to check encryption key:%" PRIi8 " in mnode leader since it's not loaded", pReq->db,
           tsEncryptionKeyStat);
    goto _exit;
  }

  int64_t curMs = taosGetTimestampMs();
  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    bool online = false;
    if ((pDnode->encryptionKeyStat != tsEncryptionKeyStat || pDnode->encryptionKeyChksum != tsEncryptionKeyChksum) &&
        (online = mndIsDnodeOnline(pDnode, curMs))) {
      code = TSDB_CODE_MND_INVALID_ENCRYPT_KEY;
      mError("db:%s, failed to check encryption key:%" PRIi8
             "-%u in dnode:%d since it's inconsitent with mnode leader:%" PRIi8 "-%u",
             pReq->db, pDnode->encryptionKeyStat, pDnode->encryptionKeyChksum, pDnode->id, tsEncryptionKeyStat,
             tsEncryptionKeyChksum);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pDnode);
      break;
    }
    sdbRelease(pSdb, pDnode);
  }
#else
  if (pReq->encryptAlgorithm != TSDB_ENCRYPT_ALGO_NONE) {
    code = TSDB_CODE_MND_INVALID_DB_OPTION;
    goto _exit;
  }
#endif
_exit:
  TAOS_RETURN(code);
}

#ifndef TD_ENTERPRISE
int32_t mndCheckDbDnodeList(SMnode *pMnode, char *db, char *dnodeListStr, SArray *dnodeList) {
  if (dnodeListStr[0] != 0) {
    terrno = TSDB_CODE_OPS_NOT_SUPPORT;
    return terrno;
  } else {
    return 0;
  }
}
#endif
#endif

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq) {
  int32_t         code = -1, lino = 0;
  SMnode         *pMnode = pReq->info.node;
  SMountObj      *pMount = NULL;
  SUserObj       *pUser = NULL;
  SCreateMountReq createReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSCreateMountReq(pReq->pCont, pReq->contLen, &createReq));
  mInfo("mount:%s, start to create on dnode %d from %s", createReq.mountName, createReq.dnodeId, createReq.mountPath);

  if ((pMount = mndAcquireMount(pMnode, createReq.mountName))) {
    if (createReq.ignoreExist) {
      mInfo("mount:%s, already exist, ignore exist is set", createReq.mountName);
      code = 0;
      goto _exit;
    } else {
      code = TSDB_CODE_MND_MOUNT_ALREADY_EXIST;
      goto _exit;
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_DB_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_MOUNT_IN_CREATING | TSDB_CODE_MND_DB_IN_DROPPING | TSDB_CODE_APP_ERROR
      goto _exit;
    }
  }

  // mount operation share the privileges of db
  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DB, NULL));
  TAOS_CHECK_EXIT(grantCheck(TSDB_GRANT_MOUNT));  // TODO: implement when the plan is ready

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));

  TAOS_CHECK_EXIT(mndCreateMount(pMnode, pReq, &createReq, pUser));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  // SName name = {0};
  // if (tNameFromString(&name, createReq.db, T_NAME_ACCT | T_NAME_DB) < 0)
  //   mError("db:%s, failed to parse db name", createReq.db);

  auditRecord(pReq, pMnode->clusterId, "createMount", createReq.mountName, "", createReq.sql, createReq.sqlLen);

_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, dnode:%d, path:%s, failed to create at line:%d since %s", createReq.mountName, createReq.dnodeId,
           createReq.mountPath, lino, tstrerror(code));
  }

  mndReleaseMount(pMnode, pMount);
  mndReleaseUser(pMnode, pUser);
  tFreeSCreateMountReq(&createReq);

  TAOS_RETURN(code);
}

#if 0
static void mndDumpDbCfgInfo(SDbCfgRsp *cfgRsp, SDbObj *pDb) {
  tstrncpy(cfgRsp->db, pDb->name, sizeof(cfgRsp->db));
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
  cfgRsp->hashMethod = pDb->cfg.hashMethod;
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
  cfgRsp->s3ChunkSize = pDb->cfg.s3ChunkSize;
  cfgRsp->s3KeepLocal = pDb->cfg.s3KeepLocal;
  cfgRsp->s3Compact = pDb->cfg.s3Compact;
  cfgRsp->withArbitrator = pDb->cfg.withArbitrator;
  cfgRsp->encryptAlgorithm = pDb->cfg.encryptAlgorithm;
  cfgRsp->compactInterval = pDb->cfg.compactInterval;
  cfgRsp->compactStartTime = pDb->cfg.compactStartTime;
  cfgRsp->compactEndTime = pDb->cfg.compactEndTime;
  cfgRsp->compactTimeOffset = pDb->cfg.compactTimeOffset;
}

static int32_t mndProcessGetDbCfgReq(SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SDbCfgReq cfgReq = {0};
  SDbCfgRsp cfgRsp = {0};

  TAOS_CHECK_GOTO(tDeserializeSDbCfgReq(pReq->pCont, pReq->contLen, &cfgReq), NULL, _exit);

  if (strcasecmp(cfgReq.db, TSDB_INFORMATION_SCHEMA_DB) && strcasecmp(cfgReq.db, TSDB_PERFORMANCE_SCHEMA_DB)) {
    pDb = mndAcquireDb(pMnode, cfgReq.db);
    if (pDb == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _exit;
    }

    mndDumpDbCfgInfo(&cfgRsp, pDb);
  }

  int32_t contLen = tSerializeSDbCfgRsp(NULL, 0, &cfgRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  int32_t ret = 0;
  if ((ret = tSerializeSDbCfgRsp(pRsp, contLen, &cfgRsp)) < 0) {
    code = ret;
    goto _exit;
  }

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  code = 0;

_exit:

  tFreeSDbCfgRsp(&cfgRsp);

  if (code != 0) {
    mError("db:%s, failed to get cfg since %s", cfgReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);

  TAOS_RETURN(code);
}
#endif

#if 0
static int32_t mndSetDropDbPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndDbActionEncode(pDb);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendPrepareLog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SArbGroup *pArbGroup = NULL;
    pIter = sdbFetch(pSdb, SDB_ARBGROUP, pIter, (void **)&pArbGroup);
    if (pIter == NULL) break;

    if (pArbGroup->dbUid == pDb->uid) {
      if ((code = mndSetDropArbGroupPrepareLogs(pTrans, pArbGroup)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pArbGroup);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pArbGroup);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetDropDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndDbActionEncode(pDb);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SArbGroup *pArbGroup = NULL;
    pIter = sdbFetch(pSdb, SDB_ARBGROUP, pIter, (void **)&pArbGroup);
    if (pIter == NULL) break;

    if (pArbGroup->dbUid == pDb->uid) {
      if ((code = mndSetDropArbGroupCommitLogs(pTrans, pArbGroup)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pArbGroup);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pArbGroup);
  }

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
      if (pVgRaw == NULL) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }
      if ((code = mndTransAppendCommitlog(pTrans, pVgRaw)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }
      if ((code = sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->dbUid == pDb->uid) {
      SSdbRaw *pStbRaw = mndStbActionEncode(pStb);
      if (pStbRaw == NULL) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pStbRaw);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        return -1;
      }
      if ((code = mndTransAppendCommitlog(pTrans, pStbRaw)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pStbRaw);
        return -1;
      }
      if ((code = sdbSetRawStatus(pStbRaw, SDB_STATUS_DROPPED)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pStbRaw);
        return -1;
      }
    }

    sdbRelease(pSdb, pStb);
  }

  TAOS_RETURN(code);
}

static int32_t mndBuildDropVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t code = 0;
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, true));
  }

  TAOS_RETURN(code);
}

static int32_t mndSetDropDbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      if ((code = mndBuildDropVgroupAction(pMnode, pTrans, pDb, pVgroup)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pVgroup);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

static int32_t mndBuildDropDbRsp(SDbObj *pDb, int32_t *pRspLen, void **ppRsp, bool useRpcMalloc) {
  int32_t    code = 0;
  SDropDbRsp dropRsp = {0};
  if (pDb != NULL) {
    (void)memcpy(dropRsp.db, pDb->name, TSDB_DB_FNAME_LEN);
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
    code = terrno;
    TAOS_RETURN(code);
  }

  int32_t ret = 0;
  if ((ret = tSerializeSDropDbRsp(pRsp, rspLen, &dropRsp)) < 0) return ret;
  *pRspLen = rspLen;
  *ppRsp = pRsp;
  TAOS_RETURN(code);
}

static int32_t mndDropDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-db");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  mInfo("trans:%d start to drop db:%s", pTrans->id, pDb->name);

  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _exit);

  if (mndTopicExistsForDb(pMnode, pDb)) {
    code = TSDB_CODE_MND_TOPIC_MUST_BE_DELETED;
    goto _exit;
  }

  TAOS_CHECK_GOTO(mndSetDropDbPrepareLogs(pMnode, pTrans, pDb), NULL, _exit);
  TAOS_CHECK_GOTO(mndSetDropDbCommitLogs(pMnode, pTrans, pDb), NULL, _exit);
  TAOS_CHECK_GOTO(mndDropStreamByDb(pMnode, pTrans, pDb), NULL, _exit);
#ifdef TD_ENTERPRISE
  TAOS_CHECK_GOTO(mndDropViewByDb(pMnode, pTrans, pDb), NULL, _exit);
#endif
  TAOS_CHECK_GOTO(mndDropSmasByDb(pMnode, pTrans, pDb), NULL, _exit);
  TAOS_CHECK_GOTO(mndDropIdxsByDb(pMnode, pTrans, pDb), NULL, _exit);
  TAOS_CHECK_GOTO(mndStreamSetStopStreamTasksActions(pMnode, pTrans, pDb->uid), NULL, _exit);
  TAOS_CHECK_GOTO(mndSetDropDbRedoActions(pMnode, pTrans, pDb), NULL, _exit);
  TAOS_CHECK_GOTO(mndUserRemoveDb(pMnode, pTrans, pDb->name), NULL, _exit);

  int32_t rspLen = 0;
  void   *pRsp = NULL;
  TAOS_CHECK_GOTO(mndBuildDropDbRsp(pDb, &rspLen, &pRsp, false), NULL, _exit);
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _exit);
  code = 0;

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
#endif
static int32_t mndProcessDropMountReq(SRpcMsg *pReq) {
    fprintf(stderr, "mndProcessDropMountReq\n");
    return 0;
#if 0
  SMnode    *pMnode = pReq->info.node;
  int32_t    code = -1;
  SDbObj    *pDb = NULL;
  SDropDbReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropDbReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _exit);

  mInfo("db:%s, start to drop", dropReq.db);

  pDb = mndAcquireDb(pMnode, dropReq.db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    if (dropReq.ignoreNotExists) {
      code = mndBuildDropDbRsp(pDb, &pReq->info.rspLen, &pReq->info.rsp, true);
    }
    goto _exit;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_DB, pDb), NULL, _exit);

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      bool isFound = false;
      for (int32_t i = 0; i < pVgroup->replica; i++) {
        if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_OFFLINE) {
          isFound = true;
          break;
        }
      }
      if (!isFound) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }
      code = TSDB_CODE_MND_VGROUP_OFFLINE;
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      goto _exit;
    }

    sdbRelease(pSdb, pVgroup);
  }

  code = mndDropDb(pMnode, pReq, pDb);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  SName name = {0};
  if (tNameFromString(&name, dropReq.db, T_NAME_ACCT | T_NAME_DB) < 0)
    mError("db:%s, failed to parse db name", dropReq.db);

  auditRecord(pReq, pMnode->clusterId, "dropDB", name.dbname, "", dropReq.sql, dropReq.sqlLen);

_exit:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop since %s", dropReq.db, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSDropDbReq(&dropReq);
  TAOS_RETURN(code);
#endif
}

#if 0
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

void mndBuildDBVgroupInfo(SDbObj *pDb, SMnode *pMnode, SArray *pVgList) {
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
          (void)memcpy(pEp->fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
          pEp->port = pDnode->port;
        }
        mndReleaseDnode(pMnode, pDnode);
        if (pVgid->syncState == TAOS_SYNC_STATE_LEADER || pVgid->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
          vgInfo.epSet.inUse = gid;
        }
      }
      vindex++;
      if (taosArrayPush(pVgList, &vgInfo) == NULL) {
        mError("db:%s, failed to push vgInfo to array, vgId:%d, but continue next", pDb->name, vgInfo.vgId);
      }
    }

    sdbRelease(pSdb, pVgroup);

    if (pDb && (vindex >= pDb->cfg.numOfVgroups)) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }
}

int32_t mndExtractDbInfo(SMnode *pMnode, SDbObj *pDb, SUseDbRsp *pRsp, const SUseDbReq *pReq) {
  int32_t code = 0;
  pRsp->pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
  if (pRsp->pVgroupInfos == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

  if (pReq == NULL || pReq->vgVersion < pDb->vgVersion || pReq->dbId != pDb->uid || numOfTable != pReq->numOfTable ||
      pReq->stateTs < pDb->stateTs) {
    mndBuildDBVgroupInfo(pDb, pMnode, pRsp->pVgroupInfos);
  }

  (void)memcpy(pRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
  pRsp->uid = pDb->uid;
  pRsp->vgVersion = pDb->vgVersion;
  pRsp->stateTs = pDb->stateTs;
  pRsp->vgNum = taosArrayGetSize(pRsp->pVgroupInfos);
  pRsp->hashMethod = pDb->cfg.hashMethod;
  pRsp->hashPrefix = pDb->cfg.hashPrefix;
  pRsp->hashSuffix = pDb->cfg.hashSuffix;
  TAOS_RETURN(code);
}

static int32_t mndProcessUseDbReq(SRpcMsg *pReq) {
  SMnode   *pMnode = pReq->info.node;
  int32_t   code = -1;
  SDbObj   *pDb = NULL;
  SUseDbReq usedbReq = {0};
  SUseDbRsp usedbRsp = {0};

  TAOS_CHECK_GOTO(tDeserializeSUseDbReq(pReq->pCont, pReq->contLen, &usedbReq), NULL, _exit);

  char *p = strchr(usedbReq.db, '.');
  if (p && ((0 == strcmp(p + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(p + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
    (void)memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
    int32_t vgVersion = mndGetGlobalVgroupVersion(pMnode);
    if (usedbReq.vgVersion < vgVersion) {
      usedbRsp.pVgroupInfos = taosArrayInit(10, sizeof(SVgroupInfo));
      if (usedbRsp.pVgroupInfos == NULL) goto _exit;

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
      (void)memcpy(usedbRsp.db, usedbReq.db, TSDB_DB_FNAME_LEN);
      usedbRsp.uid = usedbReq.dbId;
      usedbRsp.vgVersion = usedbReq.vgVersion;
      usedbRsp.errCode = terrno;

      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _exit;
    } else {
      TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_USE_DB, pDb), NULL, _exit);

      TAOS_CHECK_GOTO(mndExtractDbInfo(pMnode, pDb, &usedbRsp, &usedbReq), NULL, _exit);

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
    goto _exit;
  }

  int32_t ret = 0;
  if ((ret = tSerializeSUseDbRsp(pRsp, contLen, &usedbRsp)) < 0) {
    code = ret;
    goto _exit;
  }

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to process use db req since %s", usedbReq.db, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  tFreeSUsedbRsp(&usedbRsp);

  TAOS_RETURN(code);
}

int32_t mndValidateDbInfo(SMnode *pMnode, SDbCacheInfo *pDbs, int32_t numOfDbs, void **ppRsp, int32_t *pRspLen) {
  int32_t       code = 0;
  SDbHbBatchRsp batchRsp = {0};
  batchRsp.pArray = taosArrayInit(numOfDbs, sizeof(SDbHbRsp));
  if (batchRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < numOfDbs; ++i) {
    SDbCacheInfo *pDbCacheInfo = &pDbs[i];
    pDbCacheInfo->dbId = be64toh(pDbCacheInfo->dbId);
    pDbCacheInfo->vgVersion = htonl(pDbCacheInfo->vgVersion);
    pDbCacheInfo->cfgVersion = htonl(pDbCacheInfo->cfgVersion);
    pDbCacheInfo->numOfTable = htonl(pDbCacheInfo->numOfTable);
    pDbCacheInfo->stateTs = be64toh(pDbCacheInfo->stateTs);
    pDbCacheInfo->tsmaVersion = htonl(pDbCacheInfo->tsmaVersion);

    SDbHbRsp rsp = {0};
    (void)memcpy(rsp.db, pDbCacheInfo->dbFName, TSDB_DB_FNAME_LEN);
    rsp.dbId = pDbCacheInfo->dbId;

    if ((0 == strcasecmp(pDbCacheInfo->dbFName, TSDB_INFORMATION_SCHEMA_DB) ||
         (0 == strcasecmp(pDbCacheInfo->dbFName, TSDB_PERFORMANCE_SCHEMA_DB)))) {
      int32_t vgVersion = mndGetGlobalVgroupVersion(pMnode);
      if (pDbCacheInfo->vgVersion >= vgVersion) {
        continue;
      }

      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      (void)memcpy(rsp.useDbRsp->db, pDbCacheInfo->dbFName, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->pVgroupInfos = taosArrayInit(10, sizeof(SVgroupInfo));

      mndBuildDBVgroupInfo(NULL, pMnode, rsp.useDbRsp->pVgroupInfos);
      rsp.useDbRsp->vgVersion = vgVersion++;

      rsp.useDbRsp->vgNum = taosArrayGetSize(rsp.useDbRsp->pVgroupInfos);

      if (taosArrayPush(batchRsp.pArray, &rsp) == NULL) {
        if (terrno != 0) code = terrno;
        return code;
      }

      continue;
    }

    SDbObj *pDb = mndAcquireDb(pMnode, pDbCacheInfo->dbFName);
    if (pDb == NULL) {
      mTrace("db:%s, no exist", pDbCacheInfo->dbFName);
      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      (void)memcpy(rsp.useDbRsp->db, pDbCacheInfo->dbFName, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->uid = pDbCacheInfo->dbId;
      rsp.useDbRsp->vgVersion = -1;
      if (taosArrayPush(batchRsp.pArray, &rsp) == NULL) {
        if (terrno != 0) code = terrno;
        return code;
      }
      continue;
    }

    int32_t numOfTable = mndGetDBTableNum(pDb, pMnode);

    if (pDbCacheInfo->vgVersion >= pDb->vgVersion && pDbCacheInfo->cfgVersion >= pDb->cfgVersion &&
        numOfTable == pDbCacheInfo->numOfTable && pDbCacheInfo->stateTs == pDb->stateTs &&
        pDbCacheInfo->tsmaVersion >= pDb->tsmaVersion) {
      mTrace("db:%s, valid dbinfo, vgVersion:%d cfgVersion:%d stateTs:%" PRId64
             " numOfTables:%d, not changed vgVersion:%d cfgVersion:%d stateTs:%" PRId64 " numOfTables:%d",
             pDbCacheInfo->dbFName, pDbCacheInfo->vgVersion, pDbCacheInfo->cfgVersion, pDbCacheInfo->stateTs,
             pDbCacheInfo->numOfTable, pDb->vgVersion, pDb->cfgVersion, pDb->stateTs, numOfTable);
      mndReleaseDb(pMnode, pDb);
      continue;
    } else {
      mTrace("db:%s, valid dbinfo, vgVersion:%d cfgVersion:%d stateTs:%" PRId64
             " numOfTables:%d, changed to vgVersion:%d cfgVersion:%d stateTs:%" PRId64 " numOfTables:%d",
             pDbCacheInfo->dbFName, pDbCacheInfo->vgVersion, pDbCacheInfo->cfgVersion, pDbCacheInfo->stateTs,
             pDbCacheInfo->numOfTable, pDb->vgVersion, pDb->cfgVersion, pDb->stateTs, numOfTable);
    }

    if (pDbCacheInfo->cfgVersion < pDb->cfgVersion) {
      rsp.cfgRsp = taosMemoryCalloc(1, sizeof(SDbCfgRsp));
      mndDumpDbCfgInfo(rsp.cfgRsp, pDb);
    }

    if (pDbCacheInfo->tsmaVersion != pDb->tsmaVersion) {
      rsp.pTsmaRsp = taosMemoryCalloc(1, sizeof(STableTSMAInfoRsp));
      if (rsp.pTsmaRsp) rsp.pTsmaRsp->pTsmas = taosArrayInit(4, POINTER_BYTES);
      if (rsp.pTsmaRsp && rsp.pTsmaRsp->pTsmas) {
        bool    exist = false;
        int32_t code = mndGetDbTsmas(pMnode, 0, pDb->uid, rsp.pTsmaRsp, &exist);
        if (TSDB_CODE_SUCCESS != code) {
          mndReleaseDb(pMnode, pDb);
          if (code != TSDB_CODE_NEED_RETRY) {
            mError("db:%s, failed to get db tsmas", pDb->name);
          } else {
            mWarn("db:%s, need retry to get db tsmas", pDb->name);
          }
          taosArrayDestroyP(rsp.pTsmaRsp->pTsmas, tFreeAndClearTableTSMAInfo);
          taosMemoryFreeClear(rsp.pTsmaRsp);
          continue;
        }
        rsp.dbTsmaVersion = pDb->tsmaVersion;
        mDebug("update tsma version to %d, got tsma num: %ld", pDb->tsmaVersion, rsp.pTsmaRsp->pTsmas->size);
      }
    }

    if (pDbCacheInfo->vgVersion < pDb->vgVersion || numOfTable != pDbCacheInfo->numOfTable ||
        pDbCacheInfo->stateTs != pDb->stateTs) {
      rsp.useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
      rsp.useDbRsp->pVgroupInfos = taosArrayInit(pDb->cfg.numOfVgroups, sizeof(SVgroupInfo));
      if (rsp.useDbRsp->pVgroupInfos == NULL) {
        mndReleaseDb(pMnode, pDb);
        mError("db:%s, failed to malloc usedb response", pDb->name);
        taosArrayDestroyP(rsp.pTsmaRsp->pTsmas, tFreeAndClearTableTSMAInfo);
        taosMemoryFreeClear(rsp.pTsmaRsp);
        continue;
      }

      mndBuildDBVgroupInfo(pDb, pMnode, rsp.useDbRsp->pVgroupInfos);
      (void)memcpy(rsp.useDbRsp->db, pDb->name, TSDB_DB_FNAME_LEN);
      rsp.useDbRsp->uid = pDb->uid;
      rsp.useDbRsp->vgVersion = pDb->vgVersion;
      rsp.useDbRsp->stateTs = pDb->stateTs;
      rsp.useDbRsp->vgNum = (int32_t)taosArrayGetSize(rsp.useDbRsp->pVgroupInfos);
      rsp.useDbRsp->hashMethod = pDb->cfg.hashMethod;
      rsp.useDbRsp->hashPrefix = pDb->cfg.hashPrefix;
      rsp.useDbRsp->hashSuffix = pDb->cfg.hashSuffix;
    }

    if (taosArrayPush(batchRsp.pArray, &rsp) == NULL) {
      mndReleaseDb(pMnode, pDb);
      if (terrno != 0) code = terrno;
      return code;
    }
    mndReleaseDb(pMnode, pDb);
  }

  int32_t rspLen = tSerializeSDbHbBatchRsp(NULL, 0, &batchRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeSDbHbBatchRsp(&batchRsp);
    return -1;
  }
  int32_t ret = 0;
  if ((ret = tSerializeSDbHbBatchRsp(pRsp, rspLen, &batchRsp)) < 0) return ret;

  *ppRsp = pRsp;
  *pRspLen = rspLen;

  tFreeSDbHbBatchRsp(&batchRsp);
  TAOS_RETURN(code);
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

static const char *getEncryptAlgorithmStr(int8_t encryptAlgorithm) {
  switch (encryptAlgorithm) {
    case TSDB_ENCRYPT_ALGO_NONE:
      return TSDB_ENCRYPT_ALGO_NONE_STR;
    case TSDB_ENCRYPT_ALGO_SM4:
      return TSDB_ENCRYPT_ALGO_SM4_STR;
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
        if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER ||
            pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
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
  if (buf == NULL) {
    mError("db:%s, failed to malloc buffer", pDb->name);
    return;
  }
  int32_t code = 0;
  int32_t lino = 0;

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
        TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, buf, false), &lino, _exit);
      } else if (i == 1) {
        TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->createdTime, false), &lino, _exit);
      } else if (i == 3) {
        TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&numOfTables, false), &lino, _exit);
      } else if (i == 14) {
        TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, precVstr, false), &lino, _exit);
      } else if (i == 15) {
        TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, statusVstr, false), &lino, _exit);
      } else {
        colDataSetNULL(pColInfo, rows);
      }
    }
  } else {
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, buf, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->createdTime, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.numOfVgroups, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&numOfTables, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.replications, false), &lino, _exit);

    const char *strictStr = pDb->cfg.strict ? "on" : "off";
    char        strictVstr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(strictVstr, strictStr, 24);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)strictVstr, false), &lino, _exit);

    char    durationStr[128] = {0};
    char    durationVstr[128] = {0};
    int32_t len = formatDurationOrKeep(&durationVstr[VARSTR_HEADER_SIZE], sizeof(durationVstr) - VARSTR_HEADER_SIZE,
                                       pDb->cfg.daysPerFile);

    varDataSetLen(durationVstr, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)durationVstr, false), &lino, _exit);

    char keepVstr[128] = {0};
    char keep0Str[32] = {0};
    char keep1Str[32] = {0};
    char keep2Str[32] = {0};

    int32_t lenKeep0 = formatDurationOrKeep(keep0Str, sizeof(keep0Str), pDb->cfg.daysToKeep0);
    int32_t lenKeep1 = formatDurationOrKeep(keep1Str, sizeof(keep1Str), pDb->cfg.daysToKeep1);
    int32_t lenKeep2 = formatDurationOrKeep(keep2Str, sizeof(keep2Str), pDb->cfg.daysToKeep2);

    if (pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep1 || pDb->cfg.daysToKeep0 > pDb->cfg.daysToKeep2) {
      len = tsnprintf(&keepVstr[VARSTR_HEADER_SIZE], sizeof(keepVstr), "%s,%s,%s", keep1Str, keep2Str, keep0Str);
    } else {
      len = tsnprintf(&keepVstr[VARSTR_HEADER_SIZE], sizeof(keepVstr), "%s,%s,%s", keep0Str, keep1Str, keep2Str);
    }
    varDataSetLen(keepVstr, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)keepVstr, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.buffer, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.pageSize, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.pages, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.minRows, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.maxRows, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.compression, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)precVstr, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)statusVstr, false), &lino, _exit);

    char *rentensionVstr = buildRetension(pDb->cfg.pRetensions);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (rentensionVstr == NULL) {
      colDataSetNULL(pColInfo, rows);
    } else {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)rentensionVstr, false), &lino, _exit);
      taosMemoryFree(rentensionVstr);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.numOfStables, false), &lino, _exit);

    const char *cacheModelStr = getCacheModelStr(pDb->cfg.cacheLast);
    char        cacheModelVstr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(cacheModelVstr, cacheModelStr, 24);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)cacheModelVstr, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.cacheLastSize, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walLevel, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walFsyncPeriod, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walRetentionPeriod, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.walRetentionSize, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.sstTrigger, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int16_t hashPrefix = pDb->cfg.hashPrefix;
    if (hashPrefix > 0) {
      hashPrefix = pDb->cfg.hashPrefix - strlen(pDb->name) - 1;
    } else if (hashPrefix < 0) {
      hashPrefix = pDb->cfg.hashPrefix + strlen(pDb->name) + 1;
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&hashPrefix, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.hashSuffix, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.tsdbPageSize, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.keepTimeOffset, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.s3ChunkSize, false), &lino, _exit);

    char keeplocalVstr[128] = {0};
    len = tsnprintf(&keeplocalVstr[VARSTR_HEADER_SIZE], sizeof(keeplocalVstr), "%dm", pDb->cfg.s3KeepLocal);
    varDataSetLen(keeplocalVstr, len);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)keeplocalVstr, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.s3Compact, false), &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)&pDb->cfg.withArbitrator, false), &lino, _exit);

    const char *encryptAlgorithmStr = getEncryptAlgorithmStr(pDb->cfg.encryptAlgorithm);
    char        encryptAlgorithmVStr[24] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(encryptAlgorithmVStr, encryptAlgorithmStr, 24);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)encryptAlgorithmVStr, false), &lino, _exit);

    TAOS_UNUSED(formatDurationOrKeep(durationStr, sizeof(durationStr), pDb->cfg.compactInterval));
    STR_WITH_MAXSIZE_TO_VARSTR(durationVstr, durationStr, sizeof(durationVstr));
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, cols++))) {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)durationVstr, false), &lino, _exit);
    }

    len = formatDurationOrKeep(durationStr, sizeof(durationStr), pDb->cfg.compactStartTime);
    TAOS_UNUSED(formatDurationOrKeep(durationVstr, sizeof(durationVstr), pDb->cfg.compactEndTime));
    TAOS_UNUSED(snprintf(durationStr + len, sizeof(durationStr) - len, ",%s", durationVstr));
    STR_WITH_MAXSIZE_TO_VARSTR(durationVstr, durationStr, sizeof(durationVstr));
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, cols++))) {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)durationVstr, false), &lino, _exit);
    }

    TAOS_UNUSED(snprintf(durationStr, sizeof(durationStr), "%dh", pDb->cfg.compactTimeOffset));
    STR_WITH_MAXSIZE_TO_VARSTR(durationVstr, durationStr, sizeof(durationVstr));
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, cols++))) {
      TAOS_CHECK_GOTO(colDataSetVal(pColInfo, rows, (const char *)durationVstr, false), &lino, _exit);
    }
  }
_exit:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
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
#endif
static int32_t mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
    fprintf(stderr, "mndRetrieveMounts: %d\n", rowsCapacity);
#if 0    
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  SDbObj    *pDb = NULL;
  SUserObj  *pUser = NULL;
  ESdbStatus objStatus = 0;

  (void)mndAcquireUser(pMnode, pReq->info.conn.user, &pUser);
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
#endif
  return 0;
}

static void mndCancelGetNextMount(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_MOUNT);
}

void mndMountFreeObj(SMountObj *pObj) {
  taosMemoryFreeClear(pObj->dnodeId);
  taosMemoryFreeClear(pObj->dbObj);
  if (pObj->paths) {
    for (int32_t i = 0; i < pObj->nMounts; ++i) {
      taosMemoryFreeClear(pObj->paths[i]);
    }
    taosMemoryFreeClear(pObj->paths);
  }
}