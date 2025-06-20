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
#include "mndSsMigrate.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tmisce.h"
#include "tmsgcb.h"

#define MND_SSMIGRATE_VER_NUMBER 1
#define MND_SSMIGRATE_ID_LEN     11

static int32_t mndProcessSsMigrateDbTimer(SRpcMsg *pReq);
static int32_t mndProcessQuerySsMigrateProgressTimer(SRpcMsg *pReq);
static int32_t mndProcessQuerySsMigrateProgressRsp(SRpcMsg *pReq);
static int32_t mndProcessFollowerSsMigrateRsp(SRpcMsg *pReq);

int32_t mndInitSsMigrate(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SSMIGRATE, mndRetrieveSsMigrate);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_SSMIGRATE, mndProcessKillSsMigrateReq);
  mndSetMsgHandle(pMnode, TDMT_VND_QUERY_SSMIGRATE_PROGRESS_RSP, mndProcessQuerySsMigrateProgressRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_SSMIGRATE_DB_TIMER, mndProcessSsMigrateDbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_QUERY_SSMIGRATE_PROGRESS_TIMER, mndProcessQuerySsMigrateProgressTimer);
  mndSetMsgHandle(pMnode, TDMT_VND_FOLLOWER_SSMIGRATE_RSP, mndProcessFollowerSsMigrateRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_KILL_SSMIGRATE_RSP, mndTransProcessRsp);

  SSdbTable table = {
      .sdbType = SDB_SSMIGRATE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndSsMigrateActionEncode,
      .decodeFp = (SdbDecodeFp)mndSsMigrateActionDecode,
      .insertFp = (SdbInsertFp)mndSsMigrateActionInsert,
      .updateFp = (SdbUpdateFp)mndSsMigrateActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSsMigrateActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSsMigrate(SMnode *pMnode) { mDebug("mnd ssmigrate cleanup"); }

void tFreeSsMigrateObj(SSsMigrateObj *pSsMigrate) {
  taosArrayDestroy(pSsMigrate->vgroups);
}

int32_t tSerializeSSsMigrateObj(void *buf, int32_t bufLen, const SSsMigrateObj *pObj) {
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
    SVgroupSsMigrateDetail *pDetail = (SVgroupSsMigrateDetail *)taosArrayGet(pObj->vgroups, i);
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

int32_t tDeserializeSSsMigrateObj(void *buf, int32_t bufLen, SSsMigrateObj *pObj) {
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
    pObj->vgroups = taosArrayInit(numVnode, sizeof(SVgroupSsMigrateDetail));
  }
  for (int32_t i = 0; i < numVnode; ++i) {
    SVgroupSsMigrateDetail detail;
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

SSdbRaw *mndSsMigrateActionEncode(SSsMigrateObj *pSsMigrate) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_SUCCESS;

  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  int32_t tlen = tSerializeSSsMigrateObj(NULL, 0, pSsMigrate);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_SSMIGRATE, MND_SSMIGRATE_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  tlen = tSerializeSSsMigrateObj(buf, tlen, pSsMigrate);
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
    mError("ssmigrate:%" PRId32 ", failed to encode to raw:%p since %s", pSsMigrate->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("ssmigrate:%" PRId32 ", encode to raw:%p, row:%p", pSsMigrate->id, pRaw, pSsMigrate);
  return pRaw;
}

SSdbRow *mndSsMigrateActionDecode(SSdbRaw *pRaw) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSdbRow     *pRow = NULL;
  SSsMigrateObj *pSsMigrate = NULL;
  void        *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto OVER;
  }

  if (sver != MND_SSMIGRATE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("ssmigrate read invalid ver, data ver: %d, curr ver: %d", sver, MND_SSMIGRATE_VER_NUMBER);
    goto OVER;
  }

  pRow = sdbAllocRow(sizeof(SSsMigrateObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto OVER;
  }

  pSsMigrate = sdbGetRowObj(pRow);
  if (pSsMigrate == NULL) {
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

  if ((terrno = tDeserializeSSsMigrateObj(buf, tlen, pSsMigrate)) < 0) {
    goto OVER;
  }

OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("ssmigrate:%" PRId32 ", failed to decode from raw:%p since %s", pSsMigrate->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("ssmigrate:%" PRId32 ", decode from raw:%p, row:%p", pSsMigrate->id, pRaw, pSsMigrate);
  return pRow;
}

int32_t mndSsMigrateActionInsert(SSdb *pSdb, SSsMigrateObj *pSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform insert action", pSsMigrate->id);
  return 0;
}

int32_t mndSsMigrateActionDelete(SSdb *pSdb, SSsMigrateObj *pSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform delete action", pSsMigrate->id);
  tFreeSsMigrateObj(pSsMigrate);
  return 0;
}

int32_t mndSsMigrateActionUpdate(SSdb *pSdb, SSsMigrateObj *pOldSsMigrate, SSsMigrateObj *pNewSsMigrate) {
  mTrace("ssmigrate:%" PRId32 ", perform update action, old row:%p new row:%p", pOldSsMigrate->id, pOldSsMigrate,
         pNewSsMigrate);

  return 0;
}

SSsMigrateObj *mndAcquireSsMigrate(SMnode *pMnode, int64_t ssMigrateId) {
  SSdb        *pSdb = pMnode->pSdb;
  SSsMigrateObj *pSsMigrate = sdbAcquire(pSdb, SDB_SSMIGRATE, &ssMigrateId);
  if (pSsMigrate == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pSsMigrate;
}

void mndReleaseSsMigrate(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSsMigrate);
  pSsMigrate = NULL;
}

int32_t mndSsMigrateGetDbName(SMnode *pMnode, int32_t ssMigrateId, char *dbname, int32_t len) {
  int32_t      code = 0;
  SSsMigrateObj *pSsMigrate = mndAcquireSsMigrate(pMnode, ssMigrateId);
  if (pSsMigrate == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  tstrncpy(dbname, pSsMigrate->dbname, len);
  mndReleaseSsMigrate(pMnode, pSsMigrate);
  TAOS_RETURN(code);
}

// ssmigrate db
int32_t mndAddSsMigrateToTran(SMnode *pMnode, STrans *pTrans, SSsMigrateObj *pSsMigrate, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  pSsMigrate->dbUid = pDb->uid;
  pSsMigrate->id = tGenIdPI32();
  tstrncpy(pSsMigrate->dbname, pDb->name, sizeof(pSsMigrate->dbname));

  pSsMigrate->vgroups = taosArrayInit(8, sizeof(SVgroupSsMigrateDetail));
  if (pSsMigrate->vgroups == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    terrno = code;
    TAOS_RETURN(code);
  }

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      SVgroupSsMigrateDetail detail = {.vgId = pVgroup->vgId, .done = false };
      taosArrayPush(pSsMigrate->vgroups, &detail);
    }

    sdbRelease(pSdb, pVgroup);
  }

  SSdbRaw *pVgRaw = mndSsMigrateActionEncode(pSsMigrate);
  taosArrayDestroy(pSsMigrate->vgroups);
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

  mInfo("trans:%d, ssmigrate:%d, db:%s, has been added", pTrans->id, pSsMigrate->id, pSsMigrate->dbname);
  return 0;
}

// retrieve ssmigrate
int32_t mndRetrieveSsMigrate(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pReq->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SSsMigrateObj *pSsMigrate = NULL;
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
    pShow->pIter = sdbFetch(pSdb, SDB_SSMIGRATE, pShow->pIter, (void **)&pSsMigrate);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(
        colDataSetVal(pColInfo, numOfRows, (const char *)&pSsMigrate->id, false), pSsMigrate, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pDb != NULL || !IS_SYS_DBNAME(pSsMigrate->dbname)) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, pSsMigrate->dbname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      (void)tNameGetDbName(&name, varDataVal(tmpBuf));
    } else {
      tstrncpy(varDataVal(tmpBuf), pSsMigrate->dbname, TSDB_SHOW_SQL_LEN);
    }
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false), pSsMigrate, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pSsMigrate->startTime, false), pSsMigrate, &lino,
                        _OVER);

    numOfRows++;
    sdbRelease(pSdb, pSsMigrate);
  }

_OVER:
  if (code != 0) mError("failed to retrieve at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;
  mndReleaseDb(pMnode, pDb);
  return numOfRows;
}



int32_t mndProcessKillSsMigrateReq(SRpcMsg *pReq) {
  mError("not implemented yet");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



// update progress
static int32_t mndProcessFollowerSsMigrateRsp(SRpcMsg *pReq) {
  int32_t                  code = 0;
  TAOS_RETURN(code);
}


static void mndSendFollowerSsMigrateReq(SMnode* pMnode, SFollowerSsMigrateReq *pReq) {
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

  int32_t   reqLen = tSerializeSFollowerSsMigrateReq(NULL, 0, pReq);
  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    return;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pReq->vgId);
  int32_t ret = 0;
  if ((ret = tSerializeSFollowerSsMigrateReq((char *)pHead + sizeof(SMsgHead), reqLen, pReq)) < 0) {
    return;
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_FOLLOWER_SSMIGRATE, .pCont = pHead, .contLen = contLen};
  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("vgId:%d, failed to send follower-ssmigrate request to vnode since 0x%x", pReq->vgId, code);
  } else {
    mInfo("vgId:%d, send follower-ssmigrate request to vnode, time:%" PRId64, pReq->vgId, pReq->startTimeSec);
  }
}



static int32_t mndUpdateSsMigrateProgress(SMnode *pMnode, SRpcMsg *pReq, SQuerySsMigrateProgressRsp *rsp) {
  int32_t code = 0;
  bool inProgress = false;

  for( int32_t i = 0; i < taosArrayGetSize(rsp->pFileSetStates); i++) {
    SFileSetSsMigrateState *pState = taosArrayGet(rsp->pFileSetStates, i);
    if (pState != NULL && pState->state == FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
      inProgress = true;
    }
  }

  mndSendFollowerSsMigrateReq(pMnode, rsp);

  if (inProgress) {
    mDebug("ssmigrate:%d, vgId:%d, some filesets are still in progress.", rsp->mnodeMigrateId, rsp->vgId);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  SSsMigrateObj *pSsMigrate = mndAcquireSsMigrate(pMnode, rsp->mnodeMigrateId);
  if (pSsMigrate == NULL) {
    mError("ssmigrate:%d, failed to acquire ssmigrate since %s", rsp->mnodeMigrateId, terrstr());
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  for(int32_t i = 0; i < taosArrayGetSize(pSsMigrate->vgroups); i++) {
    SVgroupSsMigrateDetail *pDetail = taosArrayGet(pSsMigrate->vgroups, i);
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
    pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "update-ssmigrate");
    if (pTrans == NULL) {
      mError("failed to create update-ssmigrate trans since %s", terrstr());
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
  } else {
    pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, pReq, "drop-ssmigrate");
    if (pTrans == NULL) {
      mError("failed to create drop-ssmigrate trans since %s", terrstr());
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
  }

  mndTransSetDbName(pTrans, pSsMigrate->dbname, NULL);
  mInfo("trans:%d, ssmigrate:%d, vgId:%d, %s-trans created", pTrans->id, rsp->mnodeMigrateId, rsp->vgId, pTrans->opername);

  SSdbRaw *pRaw = mndSsMigrateActionEncode(pSsMigrate);
  if (pRaw == NULL) {
    mndTransDrop(pTrans);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    TAOS_RETURN(code);
  }

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    TAOS_RETURN(code);
  }

  if ((code = sdbSetRawStatus(pRaw, inProgress ? SDB_STATUS_UPDATE : SDB_STATUS_DROPPED)) != 0) {
    mndTransDrop(pTrans);
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    TAOS_RETURN(code);
  }

  mndReleaseSsMigrate(pMnode, pSsMigrate);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}



static int32_t mndProcessQuerySsMigrateProgressRsp(SRpcMsg *pMsg) {
  int32_t                  code = 0;
  if (pMsg->code != 0) {
    mError("received wrong ssmigrate response, req code is %s", tstrerror(pMsg->code));
    TAOS_RETURN(pMsg->code);
  }

  SQuerySsMigrateProgressRsp rsp = {0};
  code = tDeserializeSQuerySsMigrateProgressRsp(pMsg->pCont, pMsg->contLen, &rsp);
  if (code != 0) {
    taosArrayDestroy(rsp.pFileSetStates);
    mError("failed to deserialize vnode-query-ssmigrate-progress-rsp, ret:%d, pCont:%p, len:%d", code, pMsg->pCont,
           pMsg->contLen);
    TAOS_RETURN(code);
  }

  if (rsp.mnodeMigrateId == rsp.vnodeMigrateId) {
      mDebug("ssmigrate:%d, vgId:%d, migrate progress received", rsp.mnodeMigrateId, rsp.vgId);
  } else {
      mError("ssmigrate:%d, vgId:%d, migrate progress received, but vnode side migrate id is %d",
             rsp.mnodeMigrateId,
             rsp.vgId,
             rsp.vnodeMigrateId);
  }

  SMnode *pMnode = pMsg->info.node;
  code = mndUpdateSsMigrateProgress(pMnode, pMsg, &rsp);
  taosArrayDestroy(rsp.pFileSetStates);

  TAOS_RETURN(code);
}



void mndSendQuerySsMigrateProgressReq(SMnode *pMnode, SSsMigrateObj *pSsMigrate) {
  SSdb            *pSdb = pMnode->pSdb;
  void            *pIter = NULL;
  SQuerySsMigrateProgressReq req = { .ssMigrateId = pSsMigrate->id };
  int32_t          reqLen = tSerializeSQuerySsMigrateProgressReq(NULL, 0, &req);
  int32_t          contLen = reqLen + sizeof(SMsgHead);
  int32_t          code = 0;

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    for(int32_t i = 0; i < taosArrayGetSize(pSsMigrate->vgroups); i++) {
      SVgroupSsMigrateDetail *pDetail = taosArrayGet(pSsMigrate->vgroups, i);
      if (pDetail->nodeId != pDnode->id) {
        continue;
      }

      SMsgHead *pHead = rpcMallocCont(contLen);
      if (pHead == NULL) {
        continue;
      }
      pHead->contLen = htonl(contLen);
      pHead->vgId = htonl(pDetail->vgId);
      tSerializeSQuerySsMigrateProgressReq((char *)pHead + sizeof(SMsgHead), reqLen, &req);

      SRpcMsg rpcMsg = {.msgType = TDMT_VND_QUERY_SSMIGRATE_PROGRESS, .pCont = pHead, .contLen = contLen};

      // we need to send the msg to dnode instead of vgroup, because migration may take a long time,
      // and leader may change during the migration process, while only the initial leader vnode
      // can handle the migration progress query.
      SEpSet epSet = mndGetDnodeEpset(pDnode);
      int32_t code = tmsgSendReq(&epSet, &rpcMsg);
      if (code != 0) {
        mError("ssmigrate:%d, vgId:%d, failed to send ssmigrate-query-progress request since 0x%x",
              pSsMigrate->id,
              pDetail->vgId,
              code);
      } else {
        mInfo("ssmigrate:%d, vgId:%d, ssmigrate-query-progress request sent", pSsMigrate->id, pDetail->vgId);
      }

      break;
    }
    sdbRelease(pSdb, pDnode);
  }
}


int32_t mndSsMigrateDb(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb);

static int32_t mndProcessSsMigrateDbTimer(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  void *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) {
      break;
    }
    int32_t code = mndSsMigrateDb(pMnode, NULL, pDb);
    sdbRelease(pMnode->pSdb, pDb);
    if (code == TSDB_CODE_SUCCESS) {
      mInfo("ssmigrate db:%s, has been triggered by timer", pDb->name);
    } else {
      mError("failed to trigger ssmigrate db:%s, code:%d, %s", pDb->name, code, tstrerror(code));
    }
  }

  TAOS_RETURN(0);
}


static int32_t mndProcessQuerySsMigrateProgressTimer(SRpcMsg *pReq) {
  mTrace("start to process query ssmigrate progress timer");

  int32_t code = 0;
  SMnode* pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SSsMigrateObj *pSsMigrate = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SSMIGRATE, pIter, (void **)&pSsMigrate);
    if (pIter == NULL) {
      break;
    }
    mndSendQuerySsMigrateProgressReq(pMnode, pSsMigrate);
    sdbRelease(pSdb, pSsMigrate);
  }

  return 0;
}

int32_t mndTransProcessSsMigrateVgroupRsp(SRpcMsg *pRsp) {
  int32_t code = 0;
  SMnode *pMnode = pRsp->info.node;

  SSsMigrateVgroupRsp rsp = {0};
  code = tDeserializeSSsMigrateVgroupRsp(pRsp->pCont, pRsp->contLen, &rsp);
  mInfo("vgId:%d, ssmigrate:%d, nodeId:%d", rsp.vgId, rsp.ssMigrateId, rsp.nodeId);

  SSsMigrateObj *pSsMigrate = mndAcquireSsMigrate(pMnode, rsp.ssMigrateId);
  if (pSsMigrate == NULL) {
    mError("ssmigrate:%d, failed to acquire ssmigrate since %s", rsp.ssMigrateId, terrstr());
    return mndTransProcessRsp(pRsp);
  }

  for(int32_t i = 0; i < taosArrayGetSize(pSsMigrate->vgroups); i++) {
    SVgroupSsMigrateDetail *pDetail = taosArrayGet(pSsMigrate->vgroups, i);
    if (pDetail->vgId == rsp.vgId) {
      pDetail->nodeId = rsp.nodeId;
      break;
    }
  }

  STrans* pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB, NULL, "update-ssmigrate-nodeid");
  if (pTrans == NULL) {
    mError("failed to create update-ssmigrate-nodeid trans since %s", terrstr());
    return mndTransProcessRsp(pRsp);
  }

  mndTransSetDbName(pTrans, pSsMigrate->dbname, NULL);
  mInfo("trans:%d, ssmigrate:%d, vgId:%d, %s-trans created", pTrans->id, rsp.ssMigrateId, rsp.vgId, pTrans->opername);

  SSdbRaw *pRaw = mndSsMigrateActionEncode(pSsMigrate);
  if (pRaw == NULL) {
    mndTransDrop(pTrans);
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    return mndTransProcessRsp(pRsp);
  }

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    return mndTransProcessRsp(pRsp);
  }

  if ((code = sdbSetRawStatus(pRaw, SDB_STATUS_UPDATE)) != 0) {
    mndTransDrop(pTrans);
    mndReleaseSsMigrate(pMnode, pSsMigrate);
    return mndTransProcessRsp(pRsp);
  }

  mndReleaseSsMigrate(pMnode, pSsMigrate);

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return mndTransProcessRsp(pRsp);
  }

  mndTransDrop(pTrans);
  return mndTransProcessRsp(pRsp);
}
