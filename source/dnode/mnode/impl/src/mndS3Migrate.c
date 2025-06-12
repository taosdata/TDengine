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

void tFreeS3MigrateObj(SS3MigrateObj *pS3Migrate) {
  taosArrayDestroy(pS3Migrate->vgroups);
}

int32_t tSerializeSS3MigrateObj(void *buf, int32_t bufLen, const SS3MigrateObj *pObj) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->dbUid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbname));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pObj->startTime));
  int32_t numVnode = 0;
  if (pObj->vgroups) {
    numVnode = taosArrayGetSize(pObj->vgroups);
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numVnode));
  for (int32_t i = 0; i < numVnode; ++i) {
    SVgroupS3MigrateDetail *pDetail = (SVgroupS3MigrateDetail *)taosArrayGet(pObj->vgroups, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pDetail->vgId));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pDetail->nodeId));
    TAOS_CHECK_EXIT(tEncodeBool(&encoder, pDetail->done));
  }
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
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbname));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pObj->startTime));

  int32_t numVnode = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numVnode));
  if (pObj->vgroups) {
    taosArrayClear(pObj->vgroups);
  } else {
    pObj->vgroups = taosArrayInit(numVnode, sizeof(SVgroupS3MigrateDetail));
  }
  for (int32_t i = 0; i < numVnode; ++i) {
    SVgroupS3MigrateDetail detail;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &detail.vgId));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &detail.nodeId));
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &detail.done));
    taosArrayPush(pObj->vgroups, &detail);
  }

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
    mError("s3migrate:%" PRId32 ", failed to encode to raw:%p since %s", pS3Migrate->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("s3migrate:%" PRId32 ", encode to raw:%p, row:%p", pS3Migrate->id, pRaw, pS3Migrate);
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
    mError("s3migrate:%" PRId32 ", failed to decode from raw:%p since %s", pS3Migrate->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("s3migrate:%" PRId32 ", decode from raw:%p, row:%p", pS3Migrate->id, pRaw, pS3Migrate);
  return pRow;
}

int32_t mndS3MigrateActionInsert(SSdb *pSdb, SS3MigrateObj *pS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform insert action", pS3Migrate->id);
  return 0;
}

int32_t mndS3MigrateActionDelete(SSdb *pSdb, SS3MigrateObj *pS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform delete action", pS3Migrate->id);
  tFreeS3MigrateObj(pS3Migrate);
  return 0;
}

int32_t mndS3MigrateActionUpdate(SSdb *pSdb, SS3MigrateObj *pOldS3Migrate, SS3MigrateObj *pNewS3Migrate) {
  mTrace("s3migrate:%" PRId32 ", perform update action, old row:%p new row:%p", pOldS3Migrate->id, pOldS3Migrate,
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
int32_t mndAddS3MigrateToTran(SMnode *pMnode, STrans *pTrans, SS3MigrateObj *pS3Migrate, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  pS3Migrate->dbUid = pDb->uid;
  pS3Migrate->id = tGenIdPI32();
  tstrncpy(pS3Migrate->dbname, pDb->name, sizeof(pS3Migrate->dbname));

  pS3Migrate->vgroups = taosArrayInit(8, sizeof(SVgroupS3MigrateDetail));
  if (pS3Migrate->vgroups == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    terrno = code;
    TAOS_RETURN(code);
  }

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      SVgroupS3MigrateDetail detail = {.vgId = pVgroup->vgId, .done = false };
      taosArrayPush(pS3Migrate->vgroups, &detail);
    }

    sdbRelease(pSdb, pVgroup);
  }

  SSdbRaw *pVgRaw = mndS3MigrateActionEncode(pS3Migrate);
  if (pVgRaw == NULL) {
    taosArrayDestroy(pS3Migrate->vgroups);
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pVgRaw)) != 0) {
    taosArrayDestroy(pS3Migrate->vgroups);
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY)) != 0) {
    taosArrayDestroy(pS3Migrate->vgroups);
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }

  mInfo("trans:%d, s3migrate:%d, db:%s, has been added", pTrans->id, pS3Migrate->id, pS3Migrate->dbname);
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
    RETRIEVE_CHECK_GOTO(
        colDataSetVal(pColInfo, numOfRows, (const char *)&pS3Migrate->id, false), pS3Migrate, &lino, _OVER);

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

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) return;

    if (pVgroup->vgId == pReq->vgId) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }

  SEpSet  epSet = mndGetVgroupEpset(pMnode, pVgroup);
  sdbRelease(pSdb, pVgroup);

  int32_t   reqLen = tSerializeSFollowerS3MigrateReq(NULL, 0, pReq);
  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pReq->vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSFollowerS3MigrateReq((char *)pHead + sizeof(SMsgHead), reqLen, pReq)) < 0) {
    return;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_FOLLOWER_S3MIGRATE, .pCont = pHead, .contLen = contLen};
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("vgId:%d, failed to send follower-s3migrate request to vnode since 0x%x", pReq->vgId, code);
  } else {
    mInfo("vgId:%d, send follower-s3migrate request to vnode, time:%ld", pReq->vgId, pReq->startTimeSec);
  }
}



static int32_t mndUpdateS3MigrateProgress(SMnode *pMnode, SRpcMsg *pReq, SQueryS3MigrateProgressRsp *rsp) {
  int32_t code = 0;
  bool inProgress = false;

  for( int32_t i = 0; i < taosArrayGetSize(rsp->pFileSetStates); i++) {
    SFileSetS3MigrateState *pState = taosArrayGet(rsp->pFileSetStates, i);
    if (pState != NULL && pState->state == FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
      inProgress = true;
    }
  }

  mndSendFollowerS3MigrateReq(pMnode, rsp);

  if (inProgress) {
    mDebug("s3Migrate:%d, vgId:%d, some filesets are still in progress.", rsp->mnodeMigrateId, rsp->vgId);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  SS3MigrateObj *pS3Migrate = mndAcquireS3Migrate(pMnode, rsp->mnodeMigrateId);
  if (pS3Migrate == NULL) {
    mError("s3migrate:%d, failed to acquire s3migrate since %s", rsp->mnodeMigrateId, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  for(int32_t i = 0; i < taosArrayGetSize(pS3Migrate->vgroups); i++) {
    SVgroupS3MigrateDetail *pDetail = taosArrayGet(pS3Migrate->vgroups, i);
    if (pDetail->vgId == rsp->vgId) {
      pDetail->done = true;
    }
    if (rsp->mnodeMigrateId != rsp->vnodeMigrateId) {
      pDetail->done = true; // mark as done so that mnode will not request again
    }
    if (!pDetail->done) {
      inProgress = true;
    }
  }

  STrans *pTrans = NULL;
  if (inProgress) {
    pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "update-s3migrate");
    if (pTrans == NULL) {
      mError("failed to create update-s3migrate trans since %s", terrstr());
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
  } else {
    pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "drop-s3migrate");
    if (pTrans == NULL) {
      mError("failed to create drop-s3migrate trans since %s", terrstr());
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
  }

  mndTransSetDbName(pTrans, pS3Migrate->dbname, NULL);
  mInfo("trans:%d, s3migrate:%d, vgId:%d, %s-trans created", pTrans->id, rsp->mnodeMigrateId, rsp->vgId, pTrans->opername);

  SSdbRaw *pRaw = mndS3MigrateActionEncode(pS3Migrate);
  if (pRaw == NULL) {
    mndTransDrop(pTrans);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    TAOS_RETURN(code);
  }

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pRaw, inProgress ? SDB_STATUS_UPDATE : SDB_STATUS_DROPPED)) != 0) {
    mndTransDrop(pTrans);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    TAOS_RETURN(code);
  }

  mndReleaseS3Migrate(pMnode, pS3Migrate);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}



static int32_t mndProcessQueryS3MigrateProgressRsp(SRpcMsg *pMsg) {
  int32_t                  code = 0;
  if (pMsg->code != 0) {
    mError("received wrong s3migrate response, req code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }

  SQueryS3MigrateProgressRsp rsp = {0};
  code = tDeserializeSQueryS3MigrateProgressRsp(pMsg->pCont, pMsg->contLen, &rsp);
  if (code != 0) {
    taosArrayDestroy(rsp.pFileSetStates);
    mError("failed to deserialize vnode-query-s3migrate-progress-rsp, ret:%d, pCont:%p, len:%d", code, pMsg->pCont,
           pMsg->contLen);
    TAOS_RETURN(code);
  }

  if (rsp.mnodeMigrateId == rsp.vnodeMigrateId) {
      mDebug("s3migrate:%d, vgId:%d, migrate progress received", rsp.mnodeMigrateId, rsp.vgId);
  } else {
      mError("s3migrate:%d, vgId:%d, migrate progress received, but vnode side migrate id is %d",
             rsp.mnodeMigrateId,
             rsp.vgId,
             rsp.vnodeMigrateId);
  }

  SMnode *pMnode = pMsg->info.node;
  code = mndUpdateS3MigrateProgress(pMnode, pMsg, &rsp);
  taosArrayDestroy(rsp.pFileSetStates);

  TAOS_RETURN(code);
}



void mndSendQueryS3MigrateProgressReq(SMnode *pMnode, SS3MigrateObj *pS3Migrate) {
  SSdb            *pSdb = pMnode->pSdb;
  void            *pIter = NULL;
  SQueryS3MigrateProgressReq req = { .s3MigrateId = pS3Migrate->id };
  int32_t          reqLen = tSerializeSQueryS3MigrateProgressReq(NULL, 0, &req);
  int32_t          contLen = reqLen + sizeof(SMsgHead);
  int32_t          code = 0;

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    for(int32_t i = 0; i < taosArrayGetSize(pS3Migrate->vgroups); i++) {
      SVgroupS3MigrateDetail *pDetail = taosArrayGet(pS3Migrate->vgroups, i);
      if (pDetail->nodeId != pDnode->id) {
        continue;
      }

      SMsgHead *pHead = rpcMallocCont(contLen);
      if (pHead == NULL) {
        continue;
      }
      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(pDetail->vgId);
      tSerializeSQueryS3MigrateProgressReq((char *)pHead + sizeof(SMsgHead), reqLen, &req);

      SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_S3MIGRATE_PROGRESS, .pCont = pHead, .contLen = contLen};

      // we need to send the msg to dnode instead of vgroup, because migration may take a long time,
      // and leader may change during the migration process, while only the initial leader vnode
      // can handle the migration progress query.
      SEpSet epSet = mndGetDnodeEpset(pDnode);
      int32_t code = tmsgSendReq(&epSet, &rpcMsg);
      if (code != 0) {
        mError("s3migrate:%d, vgId:%d, failed to send s3migrate-query-progress request since 0x%x",
              pS3Migrate->id,
              pDetail->vgId,
              code);
      } else {
        mInfo("s3migrate:%d, vgId:%d, s3migrate-query-progress request sent", pS3Migrate->id, pDetail->vgId);
      }

      break;
    }
    sdbRelease(pSdb, pDnode);
  }
}


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

int32_t mndTransProcessS3MigrateVgroupRsp(SRpcMsg *pRsp) {
  int32_t code = 0;
  SMnode *pMnode = pRsp->info.node;

  SS3MigrateVgroupRsp rsp = {0};
  code = tDeserializeSS3MigrateVgroupRsp(pRsp->pCont, pRsp->contLen, &rsp);
  mInfo("vgId:%d, s3MigrateId:%d, nodeId:%d", rsp.vgId, rsp.s3MigrateId, rsp.nodeId);

  SS3MigrateObj *pS3Migrate = mndAcquireS3Migrate(pMnode, rsp.s3MigrateId);
  if (pS3Migrate == NULL) {
    mError("s3migrate:%d, failed to acquire s3migrate since %s", rsp.s3MigrateId, terrstr());
    return mndTransProcessRsp(pRsp);
  }

  for(int32_t i = 0; i < taosArrayGetSize(pS3Migrate->vgroups); i++) {
    SVgroupS3MigrateDetail *pDetail = taosArrayGet(pS3Migrate->vgroups, i);
    if (pDetail->vgId == rsp.vgId) {
      pDetail->nodeId = rsp.nodeId;
      break;
    }
  }

  STrans* pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, NULL, "update-s3migrate-nodeid");
  if (pTrans == NULL) {
    mError("failed to create update-s3migrate-nodeid trans since %s", terrstr());
    return mndTransProcessRsp(pRsp);
  }

  mndTransSetDbName(pTrans, pS3Migrate->dbname, NULL);
  mInfo("trans:%d, s3migrate:%d, vgId:%d, %s-trans created", pTrans->id, rsp.s3MigrateId, rsp.vgId, pTrans->opername);

  SSdbRaw *pRaw = mndS3MigrateActionEncode(pS3Migrate);
  if (pRaw == NULL) {
    mndTransDrop(pTrans);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    return mndTransProcessRsp(pRsp);
  }

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    return mndTransProcessRsp(pRsp);
  }

  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_UPDATE)) != 0) {
    mndTransDrop(pTrans);
    mndReleaseS3Migrate(pMnode, pS3Migrate);
    return mndTransProcessRsp(pRsp);
  }

  mndReleaseS3Migrate(pMnode, pS3Migrate);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return mndTransProcessRsp(pRsp);
  }

  mndTransDrop(pTrans);
  return mndTransProcessRsp(pRsp);
}
