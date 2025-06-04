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
#include "audit.h"
#include "mndS3Migrate.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#define MND_S3MIGRATE_VER_NUMBER 1
#define MND_S3MIGRATE_ID_LEN     11

static int32_t mndProcessS3MigrateDbTimer(SRpcMsg *pReq);
static int32_t mndProcessQueryS3MigrateProgressTimer(SRpcMsg *pReq);
static int32_t mndProcessQueryS3MigrateProgressRsp(SRpcMsg *pReq);
static int32_t mndProcessFollowerS3MigrateRsp(SRpcMsg *pReq);

int32_t mndInitS3Migrate(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_S3MIGRATE, mndRetrieveS3Migrate);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_S3MIGRATE, mndProcessKillS3MigrateReq);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_S3MIGRATE_PROGRESS_RSP, mndProcessQueryS3MigrateProgressRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_S3MIGRATE_DB_TIMER, mndProcessS3MigrateDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_QUERY_S3MIGRATE_PROGRESS_TIMER, mndProcessQueryS3MigrateProgressTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_FOLLOWER_S3MIGRATE_RSP, mndProcessFollowerS3MigrateRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_S3MIGRATE_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_S3MIGRATE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndS3MigrateActionEncode,
      .decodeFp = (SdbDecodeFp)mndS3MigrateActionDecode,
      .insertFp = (SdbInsertFp)mndS3MigrateActionInsert,
      .updateFp = (SdbUpdateFp)mndS3MigrateActionUpdate,
      .deleteFp = (SdbDeleteFp)mndS3MigrateActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupS3Migrate(SMnode *pMnode) { mDebug("mnd s3migrate cleanup"); }

void tFreeS3MigrateObj(SS3MigrateObj *pS3Migrate) {}

int32_t tSerializeSS3MigrateObj(void *buf, int32_t bufLen, const SS3MigrateObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->s3MigrateId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->dbUid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbname));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSS3MigrateObj(void *buf, int32_t bufLen, SS3MigrateObj *pObj) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->s3MigrateId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbname));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

SSdbRaw *mndS3MigrateActionEncode(SS3MigrateObj *pS3Migrate) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSS3MigrateObj(NULL, 0, pS3Migrate);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_S3MIGRATE, MND_S3MIGRATE_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSS3MigrateObj(buf, tlen, pS3Migrate);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, OVER);
  SDB_SET_DATALEN(pRaw, dataPos, OVER);

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("s3migrate:%" PRId32 ", failed to encode to raw:%p since %s", pS3Migrate->s3MigrateId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("s3migrate:%" PRId32 ", encode to raw:%p, row:%p", pS3Migrate->s3MigrateId, pRaw, pS3Migrate);
  return pRaw;
}

SSdbRow *mndS3MigrateActionDecode(SSdbRaw *pRaw) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSdbRow     *pRow = NULL;
  SS3MigrateObj *pS3Migrate = NULL;
  void        *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_S3MIGRATE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("s3migrate read invalid ver, data ver: %d, curr ver: %d", sver, MND_S3MIGRATE_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SS3MigrateObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pS3Migrate = sdbGetRowObj(pRow);
  if (pS3Migrate == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, OVER);

  if ((terrno = tDeserializeSS3MigrateObj(buf, tlen, pS3Migrate)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("s3migrate:%" PRId32 ", failed to decode from raw:%p since %s", pS3Migrate->s3MigrateId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("s3migrate:%" PRId32 ", decode from raw:%p, row:%p", pS3Migrate->s3MigrateId, pRaw, pS3Migrate);
  return pRow;
}

int32_t mndS3MigrateActionInsert(SSdb *pSdb, SS3MigrateObj *pS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform insert action", pS3Migrate->s3MigrateId);
  return 0;
}

int32_t mndS3MigrateActionDelete(SSdb *pSdb, SS3MigrateObj *pS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform insert action", pS3Migrate->s3MigrateId);
  tFreeS3MigrateObj(pS3Migrate);
  return 0;
}

int32_t mndS3MigrateActionUpdate(SSdb *pSdb, SS3MigrateObj *pOldS3Migrate, SS3MigrateObj *pNewS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform update action, old row:%p new row:%p", pOldS3Migrate->s3MigrateId, pOldS3Migrate,
         pNewS3Migrate);

  return 0;
}

SS3MigrateObj *mndAcquireS3Migrate(SMnode *pMnode, int64_t s3MigrateId) {
  SSdb        *pSdb = pMnode->pSdb;
  SS3MigrateObj *pS3Migrate = sdbAcquire(pSdb, SDB_S3MIGRATE, &s3MigrateId);
  if (pS3Migrate == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pS3Migrate;
}

void mndReleaseS3Migrate(SMnode *pMnode, SS3MigrateObj *pS3Migrate) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pS3Migrate);
  pS3Migrate = NULL;
}

int32_t mndS3MigrateGetDbName(SMnode *pMnode, int32_t s3MigrateId, char *dbname, int32_t len) {
  int32_t      code = 0;
  SS3MigrateObj *pS3Migrate = mndAcquireS3Migrate(pMnode, s3MigrateId);
  if (pS3Migrate == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pS3Migrate->dbname, len);
  mndReleaseS3Migrate(pMnode, pS3Migrate);
  TAOS_RETURN(code);
}

// s3migrate db
int32_t mndAddS3MigrateToTran(SMnode *pMnode, STrans *pTrans, SS3MigrateObj *pS3Migrate, SDbObj *pDb, SS3MigrateDbRsp *rsp) {
  int32_t code = 0;
  pS3Migrate->s3MigrateId = tGenIdPI32();

  tstrncpy(pS3Migrate->dbname, pDb->name, sizeof(pS3Migrate->dbname));

  pS3Migrate->startTime = taosGetTimestampMs();
  pS3Migrate->dbUid = pDb->uid;

  SSdbRaw *pVgRaw = mndS3MigrateActionEncode(pS3Migrate);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pVgRaw)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  rsp->s3MigrateId = pS3Migrate->s3MigrateId;

  return 0;
}

// retrieve s3migrate
int32_t mndRetrieveS3Migrate(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pReq->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SS3MigrateObj *pS3Migrate = NULL;
  char        *sep = NULL;
  SDbObj      *pDb = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;

  if (strlen(pShow->db) > 0) {
    sep = strchr(pShow->db, '.');
    if (sep &&
        ((0 == strcmp(sep + 1, TSDB_INFORMATION_SCHEMA_DB) || (0 == strcmp(sep + 1, TSDB_PERFORMANCE_SCHEMA_DB))))) {
      sep++;
    } else {
      pDb = mndAcquireDb(pMnode, pShow->db);
      if (pDb == NULL) return terrno;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_S3MIGRATE, pShow->pIter, (void **)&pS3Migrate);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pS3Migrate->s3MigrateId, false), pS3Migrate, &lino,
                        _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pS3Migrate->dbname)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pS3Migrate->dbname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pS3Migrate->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pS3Migrate, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pS3Migrate->startTime, false), pS3Migrate, &lino,
                        _OVER);

    numOfRows++;
    sdbRelease(pSdb, pS3Migrate);
  }

_OVER:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}



int32_t mndProcessKillS3MigrateReq(SRpcMsg *pReq) {
  mError("not implemented yet");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



// update progress
static int32_t mndProcessFollowerS3MigrateRsp(SRpcMsg *pReq) {
  int32_t                  code = 0;
  TAOS_RETURN(code);
}


static void mndSendFollowerS3MigrateReq(SMnode* pMnode, SFollowerS3MigrateReq *pReq) {
  SSdb            *pSdb = pMnode->pSdb;
  SVgObj          *pVgroup = NULL;
  void            *pIter = NULL;
  int32_t          reqLen = tSerializeSFollowerS3MigrateReq(NULL, 0, pReq);
  int32_t          contLen = reqLen + sizeof(SMsgHead);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) return;

    if (pVgroup->vgId == pReq->vgId) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }

  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    sdbRelease(pSdb, pVgroup);
    return;
  }
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSFollowerS3MigrateReq((char *)pHead + sizeof(SMsgHead), contLen, pReq)) < 0) {
    sdbRelease(pSdb, pVgroup);
    return;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_FOLLOWER_S3MIGRATE, .pCont = pHead, .contLen = contLen};
  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("vgId:%d, failed to send follower-s3migrate request to vnode since 0x%x", pVgroup->vgId, code);
  } else {
    mInfo("vgId:%d, send follower-s3migrate request to vnode, time:%ld", pVgroup->vgId, pReq->startTimeSec);
  }
  sdbRelease(pSdb, pVgroup);
}


static int32_t mndUpdateS3MigrateProgress(SMnode *pMnode, SRpcMsg *pReq, int32_t s3MigrateId, SQueryS3MigrateProgressRsp *rsp) {
  int32_t code = 0;
  bool inProgress = false;

  for( int32_t i = 0; i < taosArrayGetSize(rsp->pFileSetStates); i++) {
    SFileSetS3MigrateState *pState = taosArrayGet(rsp->pFileSetStates, i);
    if (pState != NULL && pState->state == FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
      inProgress = true;
    }
  }

  mndSendFollowerS3MigrateReq(pMnode, rsp);

  if (!inProgress) {
    // TODO: close the transaction
    mDebug("s3migrate:%d, all filesets finished, no need to update progress", s3MigrateId);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  #if 0
  void *pIter = NULL;
  while (1) {
    SS3MigrateDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->s3MigrateId == s3MigrateId && pDetail->vgId == rsp->vgId && pDetail->dnodeId == rsp->dnodeId) {
      pDetail->newNumberFileset = rsp->numberFileset;
      pDetail->newFinished = rsp->finished;
      pDetail->progress = rsp->progress;
      pDetail->remainingTime = rsp->remainingTime;

      sdbCancelFetch(pMnode->pSdb, pIter);
      sdbRelease(pMnode->pSdb, pDetail);

      TAOS_RETURN(code);
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }
    #endif

  return TSDB_CODE_MND_S3MIGRATE_DETAIL_NOT_EXIST;
}

static int32_t mndProcessQueryS3MigrateProgressRsp(SRpcMsg *pMsg) {
  int32_t                  code = 0;
  SQueryS3MigrateProgressRsp rsp = {0};
  if (pMsg->code != 0) {
    mError("received wrong s3migrate response, req code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }
  code = tDeserializeSQueryS3MigrateProgressRsp(pMsg->pCont, pMsg->contLen, &rsp);
  if (code != 0) {
    taosArrayDestroy(rsp.pFileSetStates);
    mError("failed to deserialize vnode-query-s3migrate-progress-rsp, ret:%d, pCont:%p, len:%d", code, pMsg->pCont,
           pMsg->contLen);
    TAOS_RETURN(code);
  }

  mDebug("s3migrate:%d, receive query response", rsp.s3MigrateId);

  SMnode *pMnode = pMsg->info.node;

  code = mndUpdateS3MigrateProgress(pMnode, pMsg, rsp.s3MigrateId, &rsp);
  taosArrayDestroy(rsp.pFileSetStates);

  if (code != 0) {
    mError("s3migrate:%d, failed to update progress", rsp.s3MigrateId);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}


void mndSendQueryS3MigrateProgressReq(SMnode *pMnode, SS3MigrateObj *pS3Migrate) {
  SSdb            *pSdb = pMnode->pSdb;
  SVgObj          *pVgroup = NULL;
  void            *pIter = NULL;
  SQueryS3MigrateProgressReq req = {
    .s3MigrateId = pS3Migrate->s3MigrateId,
    .timestamp = taosGetTimestampSec(),
  };
  int32_t          reqLen = tSerializeSQueryS3MigrateProgressReq(NULL, 0, &req);
  int32_t          contLen = reqLen + sizeof(SMsgHead);
  int32_t          code = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid != pS3Migrate->dbUid) continue;

    SMsgHead *pHead = rpcMallocCont(contLen);
    if (pHead == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(pVgroup->vgId);
    int32_t ret = 0;
    if ((ret = tSerializeSQueryS3MigrateProgressReq((char *)pHead + sizeof(SMsgHead), contLen, &req)) < 0) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return;
    }

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_S3MIGRATE_PROGRESS, .pCont = pHead, .contLen = contLen};
    SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
    int32_t code = tmsgSendReq(&epSet, &rpcMsg);
    if (code != 0) {
      mError("vgId:%d, failed to send s3migrate-query-progress request to vnode since 0x%x", pVgroup->vgId, code);
    } else {
      mInfo("vgId:%d, send s3migrate-query-progress request to vnode, time:%ld", pVgroup->vgId, req.timestamp);
    }
    sdbRelease(pSdb, pVgroup);
  }
}

#if 0

static int32_t mndSaveS3MigrateProgress(SMnode *pMnode, int32_t s3MigrateId) {
  int32_t code = 0;
  bool    needSave = false;
  void   *pIter = NULL;
  while (1) {
    SS3MigrateDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->s3MigrateId == s3MigrateId) {
      mDebug(
          "s3migrate:%d, check save progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->s3MigrateId, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      // these 2 number will jump back after dnode restart, so < is not used here
      if (pDetail->numberFileset != pDetail->newNumberFileset || pDetail->finished != pDetail->newFinished)
        needSave = true;
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  char dbname[TSDB_TABLE_FNAME_LEN] = {0};
  TAOS_CHECK_RETURN(mndS3MigrateGetDbName(pMnode, s3MigrateId, dbname, TSDB_TABLE_FNAME_LEN));

  if (!mndDbIsExist(pMnode, dbname)) {
    needSave = true;
    mWarn("s3migrate:%" PRId32 ", no db exist, set needSave:%s", s3MigrateId, dbname);
  }

  if (!needSave) {
    mDebug("s3migrate:%" PRId32 ", no need to save", s3MigrateId);
    TAOS_RETURN(code);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, NULL, "update-s3migrate-progress");
  if (pTrans == NULL) {
    mError("trans:%" PRId32 ", failed to create since %s", pTrans->id, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("s3migrate:%d, trans:%d, used to update s3migrate progress.", s3MigrateId, pTrans->id);

  mndTransSetDbName(pTrans, dbname, NULL);

  pIter = NULL;
  while (1) {
    SS3MigrateDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->s3MigrateId == s3MigrateId) {
      mInfo(
          "s3migrate:%d, trans:%d, check s3migrate progress, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d, "
          "newNumberFileset:%d, newFinished:%d",
          pDetail->s3MigrateId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished,
          pDetail->newNumberFileset, pDetail->newFinished);

      pDetail->numberFileset = pDetail->newNumberFileset;
      pDetail->finished = pDetail->newFinished;

      SSdbRaw *pCommitRaw = mndS3MigrateDetailActionEncode(pDetail);
      if (pCommitRaw == NULL) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }
      if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        mError("s3migrate:%d, trans:%d, failed to append commit log since %s", pDetail->s3MigrateId, pTrans->id, terrstr());
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }
      if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        mndTransDrop(pTrans);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  bool allFinished = true;
  pIter = NULL;
  while (1) {
    SS3MigrateDetailObj *pDetail = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE_DETAIL, pIter, (void **)&pDetail);
    if (pIter == NULL) break;

    if (pDetail->s3MigrateId == s3MigrateId) {
      mInfo("s3migrate:%d, trans:%d, check s3migrate finished, vgId:%d, dnodeId:%d, numberFileset:%d, finished:%d",
            pDetail->s3MigrateId, pTrans->id, pDetail->vgId, pDetail->dnodeId, pDetail->numberFileset, pDetail->finished);

      if (pDetail->numberFileset == -1 && pDetail->finished == -1) {
        allFinished = false;
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
      if (pDetail->numberFileset != -1 && pDetail->finished != -1 && pDetail->numberFileset != pDetail->finished) {
        allFinished = false;
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDetail);
        break;
      }
    }

    sdbRelease(pMnode->pSdb, pDetail);
  }

  if (!mndDbIsExist(pMnode, dbname)) {
    allFinished = true;
    mWarn("s3migrate:%" PRId32 ", no db exist, set all finished:%s", s3MigrateId, dbname);
  }

  if (allFinished) {
    mInfo("s3migrate:%d, all finished", s3MigrateId);
    pIter = NULL;
    while (1) {
      SS3MigrateDetailObj *pDetail = NULL;
      pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE_DETAIL, pIter, (void **)&pDetail);
      if (pIter == NULL) break;

      if (pDetail->s3MigrateId == s3MigrateId) {
        SSdbRaw *pCommitRaw = mndS3MigrateDetailActionEncode(pDetail);
        if (pCommitRaw == NULL) {
          mndTransDrop(pTrans);
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          if (terrno != 0) code = terrno;
          TAOS_RETURN(code);
        }
        if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
          mError("s3migrate:%d, trans:%d, failed to append commit log since %s", pDetail->s3MigrateId, pTrans->id,
                 terrstr());
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pDetail);
          mndTransDrop(pTrans);
          TAOS_RETURN(code);
        }
        if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
          sdbCancelFetch(pMnode->pSdb, pIter);
          sdbRelease(pMnode->pSdb, pDetail);
          mndTransDrop(pTrans);
          TAOS_RETURN(code);
        }
        mInfo("s3migrate:%d, add drop s3migratedetail action", pDetail->s3migrateDetailId);
      }

      sdbRelease(pMnode->pSdb, pDetail);
    }

    SS3MigrateObj *pS3Migrate = mndAcquireS3Migrate(pMnode, s3MigrateId);
    if (pS3Migrate == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    SSdbRaw *pCommitRaw = mndS3MigrateActionEncode(pS3Migrate);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    if (pCommitRaw == NULL) {
      mndTransDrop(pTrans);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
      mError("s3migrate:%d, trans:%d, failed to append commit log since %s", s3MigrateId, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) {
      mError("s3migrate:%d, trans:%d, failed to append commit log since %s", s3MigrateId, pTrans->id, terrstr());
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
    mInfo("s3migrate:%d, add drop s3migrate action", pS3Migrate->s3MigrateId);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("s3migrate:%d, trans:%d, failed to prepare since %s", s3MigrateId, pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}
#endif


static int32_t mndProcessS3MigrateDbTimer(SRpcMsg *pReq) {
  // TODO:
  return 0;
}

static int32_t mndProcessQueryS3MigrateProgressTimer(SRpcMsg *pReq) {
  mTrace("start to process query s3migrate progress timer");

  int32_t code = 0;
  SMnode* pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SS3MigrateObj *pS3Migrate = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_S3MIGRATE, pIter, (void **)&pS3Migrate);
    if (pIter == NULL) {
      break;
    }
    mndSendQueryS3MigrateProgressReq(pMnode, pS3Migrate);
    sdbRelease(pSdb, pS3Migrate);
  }

  return 0;
}


#if 0

#ifdef TD_ENTERPRISE
static int32_t mndS3MigrateDispatchAudit(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow *tw) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) {
    return 0;
  }

  SName   name = {0};
  int32_t sqlLen = 0;
  char    sql[256] = {0};
  char    skeyStr[40] = {0};
  char    ekeyStr[40] = {0};
  char   *pDbName = pDb->name;

  if (tNameFromString(&name, pDb->name, T_NAME_ACCT | T_NAME_DB) == 0) {
    pDbName = name.dbname;
  }

  if (taosFormatUtcTime(skeyStr, sizeof(skeyStr), tw->skey, pDb->cfg.precision) == 0 &&
      taosFormatUtcTime(ekeyStr, sizeof(ekeyStr), tw->ekey, pDb->cfg.precision) == 0) {
    sqlLen = tsnprintf(sql, sizeof(sql), "s3migrate db %s start with '%s' end with '%s'", pDbName, skeyStr, ekeyStr);
  } else {
    sqlLen = tsnprintf(sql, sizeof(sql), "s3migrate db %s start with %" PRIi64 " end with %" PRIi64, pDbName, tw->skey,
                       tw->ekey);
  }
  auditRecord(NULL, pMnode->clusterId, "autoS3MigrateDB", name.dbname, "", sql, sqlLen);

  return 0;
}

extern int32_t mndS3MigrateDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STimeWindow tw, SArray *vgroupIds,
                            bool metaOnly);
static int32_t mndS3MigrateDispatch(SRpcMsg *pReq) {
  int32_t code = 0;
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int64_t curMs = taosGetTimestampMs();
  int64_t curMin = curMs / 60000LL;

  void   *pIter = NULL;
  SDbObj *pDb = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb))) {
    if (pDb->cfg.s3migrateInterval <= 0) {
      mDebug("db:%p,%s, s3migrate interval is %dm, skip", pDb, pDb->name, pDb->cfg.s3migrateInterval);
      sdbRelease(pSdb, pDb);
      continue;
    }

    // daysToKeep2 would be altered
    if (pDb->cfg.s3migrateEndTime && (pDb->cfg.s3migrateEndTime <= -pDb->cfg.daysToKeep2)) {
      mWarn("db:%p,%s, s3migrate end time:%dm <= -keep2:%dm , skip", pDb, pDb->name, pDb->cfg.s3migrateEndTime,
            -pDb->cfg.daysToKeep2);
      sdbRelease(pSdb, pDb);
      continue;
    }

    int64_t s3migrateStartTime = pDb->cfg.s3migrateStartTime ? pDb->cfg.s3migrateStartTime : -pDb->cfg.daysToKeep2;
    int64_t s3migrateEndTime = pDb->cfg.s3migrateEndTime ? pDb->cfg.s3migrateEndTime : -pDb->cfg.daysPerFile;

    if (s3migrateStartTime >= s3migrateEndTime) {
      mDebug("db:%p,%s, s3migrate start time:%" PRIi64 "m >= end time:%" PRIi64 "m, skip", pDb, pDb->name,
             s3migrateStartTime, s3migrateEndTime);
      sdbRelease(pSdb, pDb);
      continue;
    }

    int64_t remainder = ((curMin - (int64_t)pDb->cfg.s3migrateTimeOffset * 60LL) % pDb->cfg.s3migrateInterval);
    if (remainder != 0) {
      mDebug("db:%p,%s, current time:%" PRIi64 "m is not divisible by s3migrate interval:%dm, offset:%" PRIi8
             "h, remainder:%" PRIi64 "m, skip",
             pDb, pDb->name, curMin, pDb->cfg.s3migrateInterval, pDb->cfg.s3migrateTimeOffset, remainder);
      sdbRelease(pSdb, pDb);
      continue;
    }

    if ((pDb->s3migrateStartTime / 60000LL) == curMin) {
      mDebug("db:%p:%s, s3migrate has already been dispatched at %" PRIi64 "m(%" PRIi64 "ms), skip", pDb, pDb->name,
             curMin, pDb->s3migrateStartTime);
      sdbRelease(pSdb, pDb);
      continue;
    }

    STimeWindow tw = {
        .skey = convertTimePrecision(curMs + s3migrateStartTime * 60000LL, TSDB_TIME_PRECISION_MILLI, pDb->cfg.precision),
        .ekey = convertTimePrecision(curMs + s3migrateEndTime * 60000LL, TSDB_TIME_PRECISION_MILLI, pDb->cfg.precision)};

    if ((code = mndS3MigrateDb(pMnode, NULL, pDb, tw, NULL, false)) == 0) {
      mInfo("db:%p,%s, succeed to dispatch s3migrate with range:[%" PRIi64 ",%" PRIi64 "], interval:%dm, start:%" PRIi64
            "m, end:%" PRIi64 "m, offset:%" PRIi8 "h",
            pDb, pDb->name, tw.skey, tw.ekey, pDb->cfg.s3migrateInterval, s3migrateStartTime, s3migrateEndTime,
            pDb->cfg.s3migrateTimeOffset);
    } else {
      mWarn("db:%p,%s, failed to dispatch s3migrate with range:[%" PRIi64 ",%" PRIi64 "], interval:%dm, start:%" PRIi64
            "m, end:%" PRIi64 "m, offset:%" PRIi8 "h, since %s",
            pDb, pDb->name, tw.skey, tw.ekey, pDb->cfg.s3migrateInterval, s3migrateStartTime, s3migrateEndTime,
            pDb->cfg.s3migrateTimeOffset, tstrerror(code));
    }

    TAOS_UNUSED(mndS3MigrateDispatchAudit(pMnode, pReq, pDb, &tw));

    sdbRelease(pSdb, pDb);
  }
  return 0;
}
#endif
#endif