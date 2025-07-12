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
#ifdef USE_MOUNT
#include "mndMount.h"
#include "audit.h"
#include "mndCompact.h"
#include "mndCompactDetail.h"
#include "mndDb.h"
#include "mndDef.h"
#include "mndDnode.h"
#include "mndIndex.h"
#include "mndIndexComm.h"
#include "mndPrivilege.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

#define MND_MOUNT_LOG_VER_NUMBER 1

SSdbRaw       *mndMountLogActionEncode(SMountLogObj *pObj);
SSdbRow       *mndMountLogActionDecode(SSdbRaw *pRaw);
static int32_t mndMountLogActionInsert(SSdb *pSdb, SMountLogObj *pObj);
static int32_t mndMountLogActionDelete(SSdb *pSdb, SMountLogObj *pObj);
static int32_t mndMountLogActionUpdate(SSdb *pSdb, SMountLogObj *pOld, SMountLogObj *pNew);

int32_t mndInitMountLog(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_MOUNT_LOG,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndMountLogActionEncode,
      .decodeFp = (SdbDecodeFp)mndMountLogActionDecode,
      .insertFp = (SdbInsertFp)mndMountLogActionInsert,
      .updateFp = (SdbUpdateFp)mndMountLogActionUpdate,
      .deleteFp = (SdbDeleteFp)mndMountLogActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMountLog(SMnode *pMnode) {}

static int32_t tSerializeSMountLogObj(void *buf, int32_t bufLen, const SMountLogObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->mountTimes));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->umountTimes));
  tEndEncode(&encoder);
  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("mountLog, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

int32_t tDeserializeSMountLogObj(void *buf, int32_t bufLen, SMountLogObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->mountTimes));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->umountTimes));
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("mountLog, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndMountLogActionEncode(SMountLogObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSMountLogObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }
  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_MOUNT_LOG, MND_MOUNT_LOG_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  tlen = tSerializeSMountLogObj(buf, tlen, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("mountLog, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }
  mTrace("mountLog, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

SSdbRow *mndMountLogActionDecode(SSdbRaw *pRaw) {
  int32_t       code = 0, lino = 0;
  SSdbRow      *pRow = NULL;
  SMountLogObj *pObj = NULL;
  void         *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_MOUNT_LOG_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("mountLog read invalid ver, data ver: %d, curr ver: %d", sver, MND_MOUNT_LOG_VER_NUMBER);
    goto _exit;
  }

  if (!(pRow = sdbAllocRow(sizeof(SMountLogObj)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSMountLogObj(buf, tlen, pObj) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  taosInitRWLatch(&pObj->lock);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("mountLog, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("mountLog, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

static int32_t mndMountLogActionInsert(SSdb *pSdb, SMountLogObj *pObj) {
  mTrace("mountLog:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndMountLogActionDelete(SSdb *pSdb, SMountLogObj *pObj) {
  mTrace("mountLog:%d, perform delete action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndMountLogActionUpdate(SSdb *pSdb, SMountLogObj *pOld, SMountLogObj *pNew) {
  mTrace("mountLog:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->mountTimes = pNew->mountTimes;
  pOld->umountTimes = pNew->umountTimes;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SMountLogObj *mndAcquireMountLog(SMnode *pMnode) {
  SSdb         *pSdb = pMnode->pSdb;
  int32_t       id = 1;
  SMountLogObj *pObj = sdbAcquire(pSdb, SDB_MOUNT_LOG, &id);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_MOUNT_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("mountLog:%d, failed to acquire mount since %s", id, terrstr());
    }
  }
  return pObj;
}

void mndReleaseMountLog(SMnode *pMnode, SMountLogObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static int32_t mndMountSetDbInfo(SMountInfo *pInfo, SMountDbInfo *pDb, SDbObj *pObj) {
  SDbCfg       *pCfg = &pObj->cfg;
  SMountVgInfo *pVg = taosArrayGet(pDb->pVgs, 0);

  // dbObj
  int32_t acctId = 0;
  char   *pDbName = strstr(pDb->dbName, ".");
  if (!pDbName) return TSDB_CODE_INVALID_PARA;
  terrno = 0;
  acctId = taosStr2Int32(pDb->dbName, NULL, 10);
  if (terrno != 0) return terrno;
  tsnprintf(pObj->name, sizeof(pObj->name), "%d.%s_%s", acctId, pInfo->mountName, pDbName + 1);
  tsnprintf(pObj->acct, sizeof(pObj->acct), "%s", TSDB_DEFAULT_USER);
  tsnprintf(pObj->createUser, sizeof(pObj->createUser), "%s", TSDB_DEFAULT_USER);
  pObj->createdTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createdTime;
  pObj->uid = pDb->dbId;  // TODO: make sure the uid is unique, add check later
  pObj->cfgVersion = 1;
  pObj->vgVersion = 1;
  pObj->tsmaVersion = 1;
  // dbCfg
  pCfg->isMount = 1;
  pCfg->numOfVgroups = taosArrayGetSize(pDb->pVgs);
  pCfg->numOfStables = 0;
  pCfg->buffer = pVg->szBuf / 1048576;  // convert to MB
  pCfg->pageSize = pVg->szPage / 1024;  // convert to KB
  pCfg->pages = pVg->szCache;
  pCfg->daysPerFile = pVg->daysPerFile;
  pCfg->daysToKeep0 = pVg->keep0;
  pCfg->daysToKeep1 = pVg->keep1;
  pCfg->daysToKeep2 = pVg->keep2;
  pCfg->keepTimeOffset = pVg->keepTimeOffset;
  pCfg->minRows = pVg->minRows;
  pCfg->maxRows = pVg->maxRows;
  pCfg->walFsyncPeriod = pVg->walFsyncPeriod;
  pCfg->walLevel = pVg->walLevel;
  pCfg->precision = pVg->precision;
  pCfg->compression = pVg->compression;
  pCfg->replications = pVg->replications;
  pCfg->strict = TSDB_DEFAULT_DB_STRICT;  // deprecated, use default value
  pCfg->cacheLast = pVg->cacheLast;
  pCfg->cacheLastSize = pVg->cacheLastSize;
  pCfg->numOfRetensions = 0;
  pCfg->schemaless = TSDB_DB_SCHEMALESS_OFF;
  pCfg->hashMethod = pVg->hashMethod;
  pCfg->hashPrefix = pVg->hashPrefix;
  pCfg->hashSuffix = pVg->hashSuffix;
  pCfg->walRetentionPeriod = pVg->walRetentionPeriod;
  pCfg->walRetentionSize = pVg->walRetentionSize;
  pCfg->walRollPeriod = pVg->walRollPeriod;
  pCfg->walSegmentSize = pVg->walSegSize;
  pCfg->sstTrigger = pVg->sttTrigger;
  pCfg->tsdbPageSize = pVg->tsdbPageSize;
  pCfg->s3ChunkSize = pVg->s3ChunkSize;
  pCfg->s3KeepLocal = pVg->s3KeepLocal;
  pCfg->s3Compact = pVg->s3Compact;
  pCfg->withArbitrator = pVg->replications == 2 ? TSDB_MAX_DB_WITH_ARBITRATOR : TSDB_MIN_DB_WITH_ARBITRATOR;
  pCfg->encryptAlgorithm = pVg->encryptAlgorithm;

  return 0;
}

static int32_t mndMountDupDbIdExist(SMnode *pMnode, SMountInfo *pInfo) {
  void   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SDbObj *pDb = NULL;
  int32_t nDbs = taosArrayGetSize(pInfo->pDbs);
  while ((pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb))) {
    if (pIter == NULL) break;
    for (int32_t i = 0; i < nDbs; ++i) {
      SMountDbInfo *pMountDb = TARRAY_GET_ELEM(pInfo->pDbs, i);
      if (pMountDb->dbId == pDb->uid) {
        mWarn("mount:%s, db:%s, dbId:%" PRId64 " is already exist", pInfo->mountName, pMountDb->dbName, pMountDb->dbId);
        sdbRelease(pSdb, pDb);
        sdbCancelFetch(pSdb, pIter);
        return TSDB_CODE_MND_MOUNT_DUP_DB_ID_EXIST;
      }
    }
    sdbRelease(pSdb, pDb);
  }

  return 0;
}

static int32_t mndMountSetVgInfo(SMnode *pMnode, SDnodeObj *pDnode, SMountInfo *pInfo, SDbObj *pDb, SMountVgInfo *pVg,
                                 SMountVgObj *pMountVg, int32_t *maxVgId) {
  SVgObj *pVgroup = &pMountVg->vg;
  pVgroup->vgId = (*maxVgId)++;
  pVgroup->createdTime = taosGetTimestampMs();
  pVgroup->updateTime = pVgroup->createdTime;
  pVgroup->version = 1;
  pVgroup->hashBegin = pVg->hashBegin;
  pVgroup->hashEnd = pVg->hashEnd;
  (void)snprintf(pVgroup->dbName, sizeof(pVgroup->dbName), "%s", pDb->name);
  pVgroup->dbUid = pDb->uid;
  pVgroup->replica = pVg->replications;
  pVgroup->mountVgId = pVg->vgId;

  pMountVg->pDb = pDb;
  pMountVg->diskPrimary = pVg->diskPrimary;
  pMountVg->committed = pVg->committed;
  pMountVg->commitID = pVg->commitID;
  pMountVg->commitTerm = pVg->commitTerm;
  pMountVg->numOfSTables = pVg->numOfSTables;
  pMountVg->numOfCTables = pVg->numOfCTables;
  pMountVg->numOfNTables = pVg->numOfNTables;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    if (pDnode->numOfVnodes >= pDnode->numOfSupportVnodes) {
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_VNODES);
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, pDb, pVgroup);
    if (pDnode->memAvail - vgMem - pDnode->memUsed <= 0) {
      mError("mount:%s, db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d, avail:%" PRId64 " used:%" PRId64,
             pInfo->mountName, pVgroup->dbName, pVgroup->vgId, vgMem, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE);
    } else {
      pDnode->memUsed += vgMem;
    }

    pVgid->dnodeId = pInfo->dnodeId;
    if (pVgroup->replica == 1) {
      pVgid->syncState = TAOS_SYNC_STATE_LEADER;
    } else {
      pVgid->syncState = TAOS_SYNC_STATE_FOLLOWER;  // TODO: support multi-replica vgroup
      mError("mount:%s, db:%s, vgId:%d, multi-replica vgroup not supported yet", pInfo->mountName, pVgroup->dbName,
             pVgroup->vgId);
      TAOS_RETURN(TSDB_CODE_OPS_NOT_SUPPORT);
    }

    mInfo("mount:%s, db:%s, vgId:%d is alloced, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
          pInfo->mountName, pVgroup->dbName, pVgroup->vgId, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);
    pDnode->numOfVnodes++;
  }
  TAOS_RETURN(0);
}

static int32_t mndMountSetStbInfo(SMnode *pMnode, SDnodeObj *pDnode, SMountInfo *pInfo, SDbObj *pDb,
                                  SMountStbInfo *pStbInfo, SStbObj *pStb) {
  SMCreateStbReq *pReq = &pStbInfo->req;
  pStb->createdTime = taosGetTimestampMs();
  pStb->updateTime = pStb->createdTime;
  snprintf(pStb->name, sizeof(pStb->name), "%s.%s", pDb->name, pReq->name);
  snprintf(pStb->db, sizeof(pStb->db), "%s", pDb->name);
  pStb->uid = pReq->suid;
  pStb->dbUid = pDb->uid;
  pStb->tagVer = pReq->tagVer;
  pStb->colVer = pReq->colVer;
  pStb->smaVer = 1;
  pStb->source = pReq->source;
  pStb->nextColId = pReq->numOfColumns + pReq->numOfTags + 1;
  pStb->keep = 0;
  pStb->ttl = 0;
  pStb->virtualStb = pReq->virtualStb;
  pStb->numOfColumns = pReq->numOfColumns;
  pStb->numOfTags = pReq->numOfTags;
  pStb->numOfFuncs = pReq->numOfFuncs;
  pStb->commentLen = pReq->commentLen;
  if (!(pStb->pColumns = taosMemoryCalloc(pReq->numOfColumns, sizeof(SSchema)))) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (!(pStb->pTags = taosMemoryCalloc(pReq->numOfTags, sizeof(SSchema)))) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (pReq->commentLen > 0) {
    if (!(pStb->comment = taosStrndup(pReq->pComment, pReq->commentLen))) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  if (!(pStb->pCmpr = taosMemoryCalloc(pReq->numOfColumns, sizeof(SColCmpr)))) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (!(pStb->pExtSchemas = taosMemoryCalloc(pReq->numOfColumns, sizeof(SExtSchema)))) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  for (int32_t c = 0; c < pReq->numOfColumns; ++c) {
    SFieldWithOptions *pColInfo = TARRAY_GET_ELEM(pReq->pColumns, c);
    void              *pColExt = TARRAY_GET_ELEM(pStbInfo->pColExts, c);
    SSchema           *pCol = pStb->pColumns + c;
    SColCmpr          *pCmpr = pStb->pCmpr + c;
    SExtSchema        *pExt = pStb->pExtSchemas + c;

    pCol->colId = *(col_id_t *)pColExt;
    pCol->type = pColInfo->type;
    pCol->bytes = pColInfo->bytes;
    pCol->flags = pColInfo->flags;
    (void)snprintf(pCol->name, sizeof(pCol->name), "%s", pColInfo->name);
  }
  for (int32_t t = 0; t < pReq->numOfTags; ++t) {
    SField  *pTagInfo = TARRAY_GET_ELEM(pReq->pTags, t);
    void    *pTagExt = TARRAY_GET_ELEM(pStbInfo->pTagExts, t);
    SSchema *pTag = pStb->pTags + t;
    pTag->colId = *(col_id_t *)pTagExt;
    pTag->type = pTagInfo->type;
    pTag->bytes = pTagInfo->bytes;
    pTag->flags = pTagInfo->flags;
    (void)snprintf(pTag->name, sizeof(pTag->name), "%s", pTagInfo->name);
  }
  pStb->pAst1 = NULL;
  pStb->pAst2 = NULL;
  taosInitRWLatch(&pStb->lock);
  mInfo("mount:%s, db:%s, stb:%s is alloced, dnode:%d", pInfo->mountName, pDb->name, pStb->name, pDnode->id);
  TAOS_RETURN(0);
}

static int32_t mndSetCreateMountPrepareActions(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  SSdbRaw *pDbRaw = mndMountActionEncode(pObj);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateDbPrepareActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDbs, int32_t nDbs) {
  for (int32_t i = 0; i < nDbs; ++i) {
    if (mndSetCreateDbPrepareAction(pMnode, pTrans, (pDbs + i)) != 0) return -1;
  }
  return 0;
}

static int32_t mndSetCreateVgPrepareActions(SMnode *pMnode, STrans *pTrans, SMountVgObj *pVgs, int32_t nVgs) {
  for (int32_t i = 0; i < nVgs; ++i) {
    if (mndAddNewVgPrepareAction(pMnode, pTrans, &((pVgs + i)->vg)) != 0) return -1;
  }
  return 0;
}

static int32_t mndSetCreateStbCommitActions(SMnode *pMnode, STrans *pTrans, SStbObj *pStbs, int32_t nStbs) {
  int32_t code = 0, lino = 0;
  char    fullIdxName[TSDB_INDEX_FNAME_LEN * 2] = {0};
  for (int32_t i = 0; i < nStbs; ++i) {
    SStbObj *pStb = pStbs + i;
    SSchema *pSchema = &(pStb->pTags[0]);
    if (mndGenIdxNameForFirstTag(fullIdxName, pStb->db, pStb->name, pSchema->name) < 0) {
      TAOS_CHECK_EXIT(terrno);
    }
    SSIdx idx = {0};
    if (mndAcquireGlobalIdx(pMnode, fullIdxName, SDB_IDX, &idx) == 0 && idx.pIdx != NULL) {
      mndReleaseIdx(pMnode, idx.pIdx);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST);
    }

    SIdxObj idxObj = {0};
    memcpy(idxObj.name, fullIdxName, TSDB_INDEX_FNAME_LEN);
    memcpy(idxObj.stb, pStb->name, TSDB_TABLE_FNAME_LEN);
    memcpy(idxObj.db, pStb->db, TSDB_DB_FNAME_LEN);
    memcpy(idxObj.colName, pSchema->name, TSDB_COL_NAME_LEN);
    idxObj.createdTime = taosGetTimestampMs();
    idxObj.uid = mndGenerateUid(fullIdxName, strlen(fullIdxName));
    idxObj.stbUid = pStb->uid;
    idxObj.dbUid = pStb->dbUid;

    TAOS_CHECK_EXIT(mndSetCreateIdxCommitLogs(pMnode, pTrans, &idxObj));
    TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));
    TAOS_CHECK_EXIT(mndSetCreateStbCommitLogs(pMnode, pTrans, NULL, pStb));
  }
_exit:
  return 0;
}

static int32_t mndSetCreateDbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDbs, int32_t nDbs) {
  int32_t code = 0;
  for (int32_t i = 0; i < nDbs; ++i) {
    SSdbRaw *pDbRaw = mndDbActionEncode(pDbs + i);
    if (pDbRaw == NULL) {
      TAOS_RETURN(terrno);
    }
    TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pDbRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_READY));
  }
  TAOS_RETURN(code);
}

static int32_t mndSetCreateVgCommitLogs(SMnode *pMnode, STrans *pTrans, SMountVgObj *pVgs, int32_t nVgs) {
  int32_t code = 0;
  for (int32_t i = 0; i < nVgs; ++i) {
    SSdbRaw *pDbRaw = mndVgroupActionEncode(&((pVgs + i)->vg));
    if (pDbRaw == NULL) {
      TAOS_RETURN(terrno);
    }
    TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pDbRaw));
    TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_READY));
  }
  TAOS_RETURN(code);
}

static int32_t mndSetCreateMountUndoLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pDbRaw = mndMountActionEncode(pObj);
  if (pDbRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pDbRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_DROPPED));

#if 0
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
#endif
  TAOS_RETURN(code);
}

static int32_t mndSetCreateMountCommitLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pDbRaw = mndMountActionEncode(pObj);
  if (pDbRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pDbRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pDbRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndBuildMountVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, SMountObj *pObj,
                                     SMountVgObj *pMountVg, int32_t *pContLen, void **ppReq) {
  int32_t          code = 0, lino = 0;
  int32_t          createLen = 0, totalLen = 0;
  void            *pBuf = NULL;
  SMountVnodeReq   mountReq = {0};
  SCreateVnodeReq *pCreateReq = &mountReq.createReq;
  pCreateReq->vgId = pVgroup->vgId;
  memcpy(pCreateReq->db, pDb->name, TSDB_DB_FNAME_LEN);
  pCreateReq->dbUid = pDb->uid;
  pCreateReq->vgVersion = pVgroup->version;
  pCreateReq->numOfStables = pDb->cfg.numOfStables;
  pCreateReq->buffer = pDb->cfg.buffer;
  pCreateReq->pageSize = pDb->cfg.pageSize;
  pCreateReq->pages = pDb->cfg.pages;
  pCreateReq->cacheLastSize = pDb->cfg.cacheLastSize;
  pCreateReq->daysPerFile = pDb->cfg.daysPerFile;
  pCreateReq->daysToKeep0 = pDb->cfg.daysToKeep0;
  pCreateReq->daysToKeep1 = pDb->cfg.daysToKeep1;
  pCreateReq->daysToKeep2 = pDb->cfg.daysToKeep2;
  pCreateReq->keepTimeOffset = pDb->cfg.keepTimeOffset;
  pCreateReq->s3ChunkSize = pDb->cfg.s3ChunkSize;
  pCreateReq->s3KeepLocal = pDb->cfg.s3KeepLocal;
  pCreateReq->s3Compact = pDb->cfg.s3Compact;
  pCreateReq->minRows = pDb->cfg.minRows;
  pCreateReq->maxRows = pDb->cfg.maxRows;
  pCreateReq->walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  pCreateReq->walLevel = pDb->cfg.walLevel;
  pCreateReq->precision = pDb->cfg.precision;
  pCreateReq->compression = pDb->cfg.compression;
  pCreateReq->strict = pDb->cfg.strict;
  pCreateReq->cacheLast = pDb->cfg.cacheLast;
  pCreateReq->replica = 0;
  pCreateReq->learnerReplica = 0;
  pCreateReq->selfIndex = -1;
  pCreateReq->learnerSelfIndex = -1;
  pCreateReq->hashBegin = pVgroup->hashBegin;
  pCreateReq->hashEnd = pVgroup->hashEnd;
  pCreateReq->hashMethod = pDb->cfg.hashMethod;
  pCreateReq->numOfRetensions = pDb->cfg.numOfRetensions;
  pCreateReq->pRetensions = pDb->cfg.pRetensions;
  pCreateReq->isTsma = pVgroup->isTsma;
  pCreateReq->pTsma = pVgroup->pTsma;
  pCreateReq->walRetentionPeriod = pDb->cfg.walRetentionPeriod;
  pCreateReq->walRetentionSize = pDb->cfg.walRetentionSize;
  pCreateReq->walRollPeriod = pDb->cfg.walRollPeriod;
  pCreateReq->walSegmentSize = pDb->cfg.walSegmentSize;
  pCreateReq->sstTrigger = pDb->cfg.sstTrigger;
  pCreateReq->hashPrefix = pDb->cfg.hashPrefix;
  pCreateReq->hashSuffix = pDb->cfg.hashSuffix;
  pCreateReq->tsdbPageSize = pDb->cfg.tsdbPageSize;
  pCreateReq->changeVersion = ++(pVgroup->syncConfChangeVer);
  pCreateReq->encryptAlgorithm = pDb->cfg.encryptAlgorithm;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica *pReplica = NULL;

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      pReplica = &pCreateReq->replicas[pCreateReq->replica];
    } else {
      pReplica = &pCreateReq->learnerReplicas[pCreateReq->learnerReplica];
    }

    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      if (pDnode->id == pVgid->dnodeId) {
        pCreateReq->selfIndex = pCreateReq->replica;
      }
    } else {
      if (pDnode->id == pVgid->dnodeId) {
        pCreateReq->learnerSelfIndex = pCreateReq->learnerReplica;
      }
    }

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      pCreateReq->replica++;
    } else {
      pCreateReq->learnerReplica++;
    }
  }
  if (pCreateReq->selfIndex == -1 && pCreateReq->learnerSelfIndex == -1) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }
  pCreateReq->changeVersion = pVgroup->syncConfChangeVer;

  // mount info
  (void)snprintf(mountReq.mountName, sizeof(mountReq.mountName), "%s", pObj->name);
  (void)snprintf(mountReq.mountPath, sizeof(mountReq.mountPath), "%s", pObj->paths[0]);
  mountReq.mountId = pObj->uid;
  mountReq.diskPrimary = pMountVg->diskPrimary;
  mountReq.mountVgId = pVgroup->mountVgId;
  mountReq.committed = pMountVg->committed;
  mountReq.commitID = pMountVg->commitID;
  mountReq.commitTerm = pMountVg->commitTerm;
  mountReq.numOfSTables = pMountVg->numOfSTables;
  mountReq.numOfCTables = pMountVg->numOfCTables;
  mountReq.numOfNTables = pMountVg->numOfNTables;

  mInfo("vgId:%d, mountVgId:%d, mountId:%" PRIi64
        ", name:%s, path:%s, build mount vnode req, replica:%d selfIndex:%d learnerReplica:%d learnerSelfIndex:%d "
        "strict:%d "
        "changeVersion:%d",
        pCreateReq->vgId, mountReq.mountVgId, mountReq.mountId, mountReq.mountName, mountReq.mountPath,
        pCreateReq->replica, pCreateReq->selfIndex, pCreateReq->learnerReplica, pCreateReq->learnerSelfIndex,
        pCreateReq->strict, pCreateReq->changeVersion);
  for (int32_t i = 0; i < pCreateReq->replica; ++i) {
    mInfo("vgId:%d, mountVgId:%d, mountId:%" PRIi64 ", replica:%d ep:%s:%u", pCreateReq->vgId, mountReq.mountVgId,
          mountReq.mountId, i, pCreateReq->replicas[i].fqdn, pCreateReq->replicas[i].port);
  }
  for (int32_t i = 0; i < pCreateReq->learnerReplica; ++i) {
    mInfo("vgId:%d, mountVgId:%d, mountId:%" PRIi64 ", replica:%d ep:%s:%u", pCreateReq->vgId, mountReq.mountVgId,
          mountReq.mountId, i, pCreateReq->learnerReplicas[i].fqdn, pCreateReq->learnerReplicas[i].port);
  }

  TAOS_CHECK_EXIT(tSerializeSMountVnodeReq(NULL, &createLen, &totalLen, &mountReq));
  TSDB_CHECK_NULL((pBuf = taosMemoryMalloc(totalLen)), code, lino, _exit, terrno);
  TAOS_CHECK_EXIT(tSerializeSMountVnodeReq(pBuf, &createLen, &totalLen, &mountReq));
_exit:
  if (code != 0) {
    mError("mount:%s, failed at line %d to build mount vnode req since %s", pObj->name, lino, tstrerror(code));
    taosMemoryFreeClear(pBuf);
    totalLen = 0;
  }
  *pContLen = totalLen;
  *ppReq = pBuf;
  TAOS_RETURN(code);
}

static int32_t mndAddMountVnodeAction(SMnode *pMnode, STrans *pTrans, SMountObj *pObj, SMountVgObj *pMountVg) {
  int32_t      code = 0, lino = 0;
  int32_t      contLen = 0;
  void        *pReq = NULL;
  STransAction action = {0};
  SVgObj      *pVg = &pMountVg->vg;
  SDbObj      *pDb = pMountVg->pDb;
  SVnodeGid   *pVgid = &pVg->vnodeGid[0];

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) TAOS_CHECK_EXIT(terrno);
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  TAOS_CHECK_EXIT(mndBuildMountVnodeReq(pMnode, pDnode, pDb, pVg, pObj, pMountVg, &contLen, &pReq));

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_MOUNT_VNODE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_EXIST;
  action.groupId = pVg->vgId;

  TAOS_CHECK_EXIT(mndTransAppendRedoAction(pTrans, &action));
_exit:
  if (code < 0) {
    mError("mount:%s, failed at line %d to add mount vnode action since %s", pObj->name, lino, tstrerror(code));
    taosMemoryFree(pReq);
  }
  TAOS_RETURN(code);
}

static int32_t mndSetCreateMountRedoActions(SMnode *pMnode, STrans *pTrans, SMountObj *pObj, SMountVgObj *pVgs,
                                            int32_t nVgs) {
  for (int32_t i = 0; i < nVgs; ++i) {
    TAOS_CHECK_RETURN(mndAddMountVnodeAction(pMnode, pTrans, pObj, pVgs + i));
  }
  TAOS_RETURN(0);
}

/**
 * Increment mount/umount times in mount log.
 * @param pMnode Pointer to the Mnode structure.
 * @param mountType Type of mount operation (0 for mount, 1 for unmount
 */
static int32_t mndIncMountTimes(SMnode *pMnode, int32_t mountType) {
  int32_t       code = 0, lino = 0;
  SMountLogObj *pMountLog = NULL;
  SMountLogObj  mountLog = {0};
  bool          release = false;
  STrans       *pTrans = NULL;

  if ((pMountLog = mndAcquireMountLog(pMnode))) {
    mountType == 0 ? ++pMountLog->mountTimes : ++pMountLog->umountTimes;
    release = true;
  } else {
    if (mountType == 1) {
      mWarn("mount log not found, cannot increment umount times");
      TAOS_CHECK_EXIT(terrno);
    }
    mountLog.id = 1;
    mountLog.createdTime = taosGetTimestampMs();
    mountLog.updateTime = mountLog.createdTime;
    mountLog.mountTimes = 1;
    mountLog.umountTimes = 0;
    pMountLog = &mountLog;
  }
  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "update-mount-log")),
                  code, lino, _exit, terrno);

  SSdbRaw *pCommitRaw = mndMountLogActionEncode(pMountLog);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (release) mndReleaseMountLog(pMnode, pMountLog);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// mount functions
int32_t mndCreateMount(SMnode *pMnode, SRpcMsg *pReq, SMountInfo *pInfo, SUserObj *pUser) {
  int32_t      code = 0, lino = 0;
  SUserObj     newUserObj = {0};
  SMountObj    mntObj = {0};
  int32_t      nDbs = 0, nVgs = 0, nStbs = 0;
  SDnodeObj   *pDnode = NULL;
  SDbObj      *pDbs = NULL;
  SMountVgObj *pVgs = NULL;
  SStbObj     *pStbs = NULL;
  STrans      *pTrans = NULL;

  (void)snprintf(mntObj.name, TSDB_MOUNT_NAME_LEN, "%s", pInfo->mountName);
  (void)snprintf(mntObj.acct, TSDB_USER_LEN, "%s", pUser->acct);
  mntObj.createdTime = taosGetTimestampMs();
  mntObj.updateTime = mntObj.createdTime;
  mntObj.uid = mndGenerateUid(mntObj.name, TSDB_MOUNT_NAME_LEN);
  (void)snprintf(mntObj.createUser, TSDB_USER_LEN, "%s", pUser->user);
  mntObj.nMounts = 1;  // currently only one mount is supported
  TSDB_CHECK_NULL((mntObj.dnodeIds = taosMemoryCalloc(mntObj.nMounts, sizeof(int32_t))), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((mntObj.paths = taosMemoryCalloc(mntObj.nMounts, sizeof(char *))), code, lino, _exit, terrno);
  mntObj.dnodeIds[0] = pInfo->dnodeId;
  TSDB_CHECK_NULL((mntObj.paths[0] = tstrndup(pInfo->mountPath, TSDB_MOUNT_PATH_LEN)), code, lino, _exit,
                  TSDB_CODE_OUT_OF_MEMORY);

  // dbCfg
  // mntObj.dbCfg = pCreate->dbCfg;
  TSDB_CHECK_CONDITION(((nDbs = taosArrayGetSize(pInfo->pDbs)) > 0), code, lino, _exit,
                       TSDB_CODE_MND_INVALID_MOUNT_INFO);

  TSDB_CHECK_NULL((pDnode = mndAcquireDnode(pMnode, pInfo->dnodeId)), code, lino, _exit, terrno);

  // check before create db
  for (int32_t i = 0; i < nDbs; ++i) {
    SMountDbInfo *pDb = taosArrayGet(pInfo->pDbs, i);
    SDbObj        dbObj = {0};
    TAOS_CHECK_EXIT(mndMountSetDbInfo(pInfo, pDb, &dbObj));
    if ((code = mndCheckDbCfg(pMnode, &dbObj.cfg)) != 0) {
      mError("mount:%s, failed to create db:%s, check db cfg failed, since %s", pInfo->mountName, pDb->dbName,
             tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
    if ((code = mndCheckDbName(dbObj.name, pUser)) != 0) {
      mError("mount:%s, failed to create db:%s, check db name failed, since %s", pInfo->mountName, pDb->dbName,
             tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
#if 0  // N/A for mount db
    if (dbObj.cfg.hashPrefix > 0) {
      int32_t dbLen = strlen(dbObj.name) + 1;
      mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix + dbLen);
      dbObj.cfg.hashPrefix += dbLen;
    } else if (dbObj.cfg.hashPrefix < 0) {
      int32_t dbLen = strlen(dbObj.name) + 1;
      mInfo("db:%s, hashPrefix adjust from %d to %d", dbObj.name, dbObj.cfg.hashPrefix, dbObj.cfg.hashPrefix - dbLen);
      dbObj.cfg.hashPrefix -= dbLen;
    }
#endif
    nVgs += taosArrayGetSize(pDb->pVgs);
    nStbs += taosArrayGetSize(pDb->pStbs);
  }
  TAOS_CHECK_EXIT(mndMountDupDbIdExist(pMnode, pInfo));

  TSDB_CHECK_NULL((pDbs = taosMemoryCalloc(nDbs, sizeof(SDbObj))), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((pVgs = taosMemoryCalloc(nVgs, sizeof(SMountVgObj))), code, lino, _exit, terrno);
  TSDB_CHECK_NULL((pStbs = taosMemoryCalloc(nStbs, sizeof(SStbObj))), code, lino, _exit, terrno);

  // create db/vg/stb
  int32_t vgIdx = 0, stbIdx = 0;
  int32_t maxVgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  if (maxVgId < 2) maxVgId = 2;
  for (int32_t i = 0; i < nDbs; ++i) {
    SMountDbInfo *pDbInfo = taosArrayGet(pInfo->pDbs, i);
    SDbObj       *pDb = &pDbs[i];
    TAOS_CHECK_EXIT(mndMountSetDbInfo(pInfo, pDbInfo, pDb));
    int32_t nDbVgs = taosArrayGetSize(pDbInfo->pVgs);
    for (int32_t v = 0; v < nDbVgs; ++v) {
      SMountVgInfo *pVgInfo = TARRAY_GET_ELEM(pDbInfo->pVgs, v);
      TAOS_CHECK_EXIT(mndMountSetVgInfo(pMnode, pDnode, pInfo, pDb, pVgInfo, &pVgs[vgIdx++], &maxVgId));
    }
    int32_t nDbStbs = taosArrayGetSize(pDbInfo->pStbs);
    for (int32_t s = 0; s < nDbStbs; ++s) {
      SMountStbInfo *pStbInfo = TARRAY_GET_ELEM(pDbInfo->pStbs, s);
      TAOS_CHECK_EXIT(mndMountSetStbInfo(pMnode, pDnode, pInfo, pDb, pStbInfo, &pStbs[stbIdx++]));
    }
  }

  // add database privileges for user
  // SUserObj *pNewUserDuped = NULL;
  // if (!pUser->superUser) {
  //   TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUserObj), NULL, _exit);
  //   TAOS_CHECK_GOTO(taosHashPut(newUserObj.readDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name,
  //   TSDB_FILENAME_LEN),
  //                   NULL, _exit);
  //   TAOS_CHECK_GOTO(taosHashPut(newUserObj.writeDbs, dbObj.name, strlen(dbObj.name) + 1, dbObj.name,
  //   TSDB_FILENAME_LEN),
  //                   NULL, _exit);
  //   pNewUserDuped = &newUserObj;
  // }

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "create-mount")), code,
                  lino, _exit, terrno);
  // mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to create mount:%s", pTrans->id, pInfo->mountName);

  mndTransSetDbName(pTrans, mntObj.name, NULL);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_CREATE_MOUNT);
  TAOS_CHECK_EXIT(mndSetCreateMountPrepareActions(pMnode, pTrans, &mntObj));
  TAOS_CHECK_EXIT(mndSetCreateDbPrepareActions(pMnode, pTrans, pDbs, nDbs));
  TAOS_CHECK_EXIT(mndSetCreateVgPrepareActions(pMnode, pTrans, pVgs, nVgs));
  TAOS_CHECK_EXIT(mndSetCreateMountRedoActions(pMnode, pTrans, &mntObj, pVgs, nVgs));
  // TAOS_CHECK_EXIT(mndSetCreateMountUndoLogs(pMnode, pTrans, &mntObj));
  TAOS_CHECK_EXIT(mndSetCreateMountCommitLogs(pMnode, pTrans, &mntObj));
  TAOS_CHECK_EXIT(mndSetCreateDbCommitLogs(pMnode, pTrans, pDbs, nDbs));
  TAOS_CHECK_EXIT(mndSetCreateVgCommitLogs(pMnode, pTrans, pVgs, nVgs));
  TAOS_CHECK_EXIT(mndSetCreateStbCommitActions(pMnode, pTrans, pStbs, nStbs));
  // TAOS_CHECK_EXIT(mndSetCreateDbUndoActions(pMnode, pTrans, &mntObj, pVgroups));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
  TAOS_CHECK_EXIT(mndIncMountTimes(pMnode, 0));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, failed at line %d to create mount, since %s", mntObj.name, lino, tstrerror(code));
  }
  mndReleaseDnode(pMnode, pDnode);
  mndMountFreeObj(&mntObj);
  mndUserFreeObj(&newUserObj);
  mndTransDrop(pTrans);
  taosMemFreeClear(pDbs);
  taosMemFreeClear(pVgs);
  if (pStbs) {
    for (int32_t i = 0; i < nStbs; ++i) {
      mndFreeStb(pStbs + i);
    }
    taosMemFreeClear(pStbs);
  }
  TAOS_RETURN(code);
}

static int32_t mndSetDropMountPrepareLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndMountActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendPrepareLog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  TAOS_RETURN(code);
}

static int32_t mndSetDropMountCommitLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndMountActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropMountDbLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t code = 0, lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;
    if (pDb->cfg.isMount) {
      const char *pDbName = strstr(pDb->name, ".");
      const char *pMountPrefix = pDbName ? strstr(pDbName + 1, pObj->name) : NULL;
      if (pMountPrefix && (pMountPrefix == (pDbName + 1)) && (pMountPrefix[strlen(pObj->name)] == '_')) {
        mInfo("db:%s, is mount db, start to drop", pDb->name);
        if ((code = mndSetDropDbPrepareLogs(pMnode, pTrans, pDb)) != 0 ||
            (code = mndSetDropDbCommitLogs(pMnode, pTrans, pDb)) != 0 ||
            (code = mndDropIdxsByDb(pMnode, pTrans, pDb)) != 0 ||
            (code = mndUserRemoveDb(pMnode, pTrans, pDb->name)) != 0 ||
            (code = mndRemoveAllStbUser(pMnode, pTrans, pDb)) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pDb);
          TAOS_CHECK_EXIT(code);
        }
      }
    }
    sdbRelease(pSdb, pDb);
  }
_exit:
  TAOS_RETURN(code);
}

#if 0
static int32_t mndBuildDropVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t code = 0;
  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, true));
  }

  TAOS_RETURN(code);
}
#endif
static int32_t mndSetDropMountRedoActions(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t code = 0, lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;
    if (pDb->cfg.isMount) {
      const char *pDbName = strstr(pDb->name, ".");
      const char *pMountPrefix = pDbName ? strstr(pDbName + 1, pObj->name) : NULL;
      if (pMountPrefix && (pMountPrefix == (pDbName + 1)) && (pMountPrefix[strlen(pObj->name)] == '_')) {
        mInfo("db:%s, is mount db, start to drop", pDb->name);
        if ((code = mndSetDropDbRedoActions(pMnode, pTrans, pDb)) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pDb);
          TAOS_CHECK_EXIT(code);
        }
      }
    }
    sdbRelease(pSdb, pDb);
  }
_exit:
  TAOS_RETURN(code);
}

static int32_t mndUserRemoveMount(SMnode *pMnode, STrans *pTrans, SMountObj *pObj) {
  int32_t code = 0, lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;
    if (pDb->cfg.isMount) {
      const char *pDbName = strstr(pDb->name, ".");
      const char *pMountPrefix = pDbName ? strstr(pDbName + 1, pObj->name) : NULL;
      if (pMountPrefix && (pMountPrefix == (pDbName + 1)) && (pMountPrefix[strlen(pObj->name)] == '_')) {
        mInfo("db:%s, is mount db, start to drop", pDb->name);
        if ((code = mndSetDropDbRedoActions(pMnode, pTrans, pDb)) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pDb);
          TAOS_CHECK_EXIT(code);
        }
      }
    }
    sdbRelease(pSdb, pDb);
  }
_exit:
  TAOS_RETURN(code);
}

int32_t mndDropMount(SMnode *pMnode, SRpcMsg *pReq, SMountObj *pObj) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "drop-mount");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  mInfo("trans:%d start to drop mount:%s", pTrans->id, pObj->name);

  mndTransSetDbName(pTrans, pObj->name, NULL);  // TODO
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  TAOS_CHECK_EXIT(mndSetDropMountPrepareLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropMountCommitLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropMountDbLogs(pMnode, pTrans, pObj));  // drop mount dbs/vgs/stbs
  TAOS_CHECK_EXIT(mndSetDropMountRedoActions(pMnode, pTrans, pObj));

  int32_t rspLen = 0;
  void   *pRsp = NULL;
  TAOS_CHECK_EXIT(mndBuildDropMountRsp(pObj, &rspLen, &pRsp, false));
  mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
  TAOS_CHECK_EXIT(mndIncMountTimes(pMnode, 1));
_exit:
  if (code != 0) {
    mError("mount:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
#endif