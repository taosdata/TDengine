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
#include "mndSma.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndStb.c"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define TSDB_SMA_VER_NUMBER   1
#define TSDB_SMA_RESERVE_SIZE 64

static SSdbRaw *mndSmaActionEncode(SSmaObj *pSma);
static SSdbRow *mndSmaActionDecode(SSdbRaw *pRaw);
static int32_t  mndSmaActionInsert(SSdb *pSdb, SSmaObj *pSma);
static int32_t  mndSmaActionDelete(SSdb *pSdb, SSmaObj *pSpSmatb);
static int32_t  mndSmaActionUpdate(SSdb *pSdb, SSmaObj *pOld, SSmaObj *pNew);
static int32_t  mndProcessMCreateSmaReq(SNodeMsg *pReq);
static int32_t  mndProcessMDropSmaReq(SNodeMsg *pReq);
static int32_t  mndProcessVCreateSmaRsp(SNodeMsg *pRsp);
static int32_t  mndProcessVDropSmaRsp(SNodeMsg *pRsp);
static int32_t  mndGetSmaMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveSma(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextSma(SMnode *pMnode, void *pIter);

int32_t mndInitSma(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_SMA,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndSmaActionEncode,
                     .decodeFp = (SdbDecodeFp)mndSmaActionDecode,
                     .insertFp = (SdbInsertFp)mndSmaActionInsert,
                     .updateFp = (SdbUpdateFp)mndSmaActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndSmaActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_SMA, mndProcessMCreateSmaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_SMA, mndProcessMDropSmaReq);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_SMA_RSP, mndProcessVCreateSmaRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_SMA_RSP, mndProcessVDropSmaRsp);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_INDEX, mndGetSmaMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_INDEX, mndRetrieveSma);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_INDEX, mndCancelGetNextSma);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSma(SMnode *pMnode) {}

static SSdbRaw *mndSmaActionEncode(SSmaObj *pSma) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SSmaObj) + pSma->exprLen + pSma->tagsFilterLen + TSDB_SMA_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_SMA, TSDB_SMA_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;

  SDB_SET_BINARY(pRaw, dataPos, pSma->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pSma->stb, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pSma->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->uid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->stbUid, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->dbUid, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->intervalUnit, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->slidingUnit, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pSma->timezone, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->dstVgId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->interval, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->offset, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pSma->sliding, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->exprLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->tagsFilterLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->sqlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pSma->astLen, _OVER)
  if (pSma->exprLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->expr, pSma->exprLen, _OVER)
  }
  if (pSma->tagsFilterLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->tagsFilter, pSma->tagsFilterLen, _OVER)
  }
  if (pSma->sqlLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->sql, pSma->sqlLen, _OVER)
  }
  if (pSma->astLen > 0) {
    SDB_SET_BINARY(pRaw, dataPos, pSma->ast, pSma->astLen, _OVER)
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_SMA_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)
  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("sma:%s, failed to encode to raw:%p since %s", pSma->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("sma:%s, encode to raw:%p, row:%p", pSma->name, pRaw, pSma);
  return pRaw;
}

static SSdbRow *mndSmaActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_SMA_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SSmaObj));
  if (pRow == NULL) goto _OVER;

  SSmaObj *pSma = sdbGetRowObj(pRow);
  if (pSma == NULL) goto _OVER;

  int32_t dataPos = 0;

  SDB_GET_BINARY(pRaw, dataPos, pSma->name, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pSma->stb, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pSma->db, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->uid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->stbUid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->dbUid, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->intervalUnit, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->slidingUnit, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pSma->timezone, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->dstVgId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->interval, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->offset, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pSma->sliding, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->exprLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->tagsFilterLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->sqlLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pSma->astLen, _OVER)

  if (pSma->exprLen > 0) {
    pSma->expr = calloc(pSma->exprLen, 1);
    if (pSma->expr == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->expr, pSma->exprLen, _OVER)
  }

  if (pSma->tagsFilterLen > 0) {
    pSma->tagsFilter = calloc(pSma->tagsFilterLen, 1);
    if (pSma->tagsFilter == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->tagsFilter, pSma->tagsFilterLen, _OVER)
  }

  if (pSma->sqlLen > 0) {
    pSma->sql = calloc(pSma->sqlLen, 1);
    if (pSma->sql == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->sql, pSma->sqlLen, _OVER)
  }

  if (pSma->astLen > 0) {
    pSma->ast = calloc(pSma->astLen, 1);
    if (pSma->ast == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pSma->ast, pSma->astLen, _OVER)
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_SMA_RESERVE_SIZE, _OVER)
  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("sma:%s, failed to decode from raw:%p since %s", pSma->name, pRaw, terrstr());
    tfree(pSma->expr);
    tfree(pSma->tagsFilter);
    tfree(pRow);
    return NULL;
  }

  mTrace("sma:%s, decode from raw:%p, row:%p", pSma->name, pRaw, pSma);
  return pRow;
}

static int32_t mndSmaActionInsert(SSdb *pSdb, SSmaObj *pSma) {
  mTrace("sma:%s, perform insert action, row:%p", pSma->name, pSma);
  return 0;
}

static int32_t mndSmaActionDelete(SSdb *pSdb, SSmaObj *pSma) {
  mTrace("sma:%s, perform delete action, row:%p", pSma->name, pSma);
  tfree(pSma->tagsFilter);
  tfree(pSma->expr);
  return 0;
}

static int32_t mndSmaActionUpdate(SSdb *pSdb, SSmaObj *pOld, SSmaObj *pNew) {
  mTrace("sma:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  return 0;
}

SSmaObj *mndAcquireSma(SMnode *pMnode, char *smaName) {
  SSdb    *pSdb = pMnode->pSdb;
  SSmaObj *pSma = sdbAcquire(pSdb, SDB_SMA, smaName);
  if (pSma == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_SMA_NOT_EXIST;
  }
  return pSma;
}

void mndReleaseSma(SMnode *pMnode, SSmaObj *pSma) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSma);
}

SDbObj *mndAcquireDbBySma(SMnode *pMnode, const char *smaName) {
  SName name = {0};
  tNameFromString(&name, smaName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static void *mndBuildVCreateSmaReq(SMnode *pMnode, SVgObj *pVgroup, SSmaObj *pSma, int32_t *pContLen) {
  SName name = {0};
  tNameFromString(&name, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SVCreateTSmaReq req = {0};
  req.tSma.version = 0;
  req.tSma.intervalUnit = pSma->intervalUnit;
  req.tSma.slidingUnit = pSma->slidingUnit;
  req.tSma.timezoneInt = pSma->timezone;
  tstrncpy(req.tSma.indexName, (char *)tNameGetTableName(&name), TSDB_INDEX_NAME_LEN);
  req.tSma.exprLen = pSma->exprLen;
  req.tSma.tagsFilterLen = pSma->tagsFilterLen;
  req.tSma.indexUid = pSma->uid;
  req.tSma.tableUid = pSma->stbUid;
  req.tSma.interval = pSma->interval;
  req.tSma.offset = pSma->offset;
  req.tSma.sliding = pSma->sliding;
  req.tSma.expr = pSma->expr;
  req.tSma.tagsFilter = pSma->tagsFilter;

  int32_t   contLen = tSerializeSVCreateTSmaReq(NULL, &req) + sizeof(SMsgHead);
  SMsgHead *pHead = malloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tSerializeSVCreateTSmaReq(&pBuf, &req);

  *pContLen = contLen;
  return pHead;
}

static void *mndBuildVDropSmaReq(SMnode *pMnode, SVgObj *pVgroup, SSmaObj *pSma, int32_t *pContLen) {
  SName name = {0};
  tNameFromString(&name, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SVDropTSmaReq req = {0};
  req.ver = 0;
  req.indexUid = pSma->uid;
  tstrncpy(req.indexName, (char *)tNameGetTableName(&name), TSDB_INDEX_NAME_LEN);

  int32_t   contLen = tSerializeSVDropTSmaReq(NULL, &req) + sizeof(SMsgHead);
  SMsgHead *pHead = malloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tDeserializeSVDropTSmaReq(&pBuf, &req);

  *pContLen = contLen;
  return pHead;
}

static int32_t mndSetCreateSmaRedoLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  SSdbRaw *pRedoRaw = mndSmaActionEncode(pSma);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateSmaCommitLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  SSdbRaw *pCommitRaw = mndSmaActionEncode(pSma);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateSmaRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SSmaObj *pSma) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateSmaReq(pMnode, pVgroup, pSma, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_CREATE_SMA;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndCreateSma(SMnode *pMnode, SNodeMsg *pReq, SMCreateSmaReq *pCreate, SDbObj *pDb, SStbObj *pStb) {
  SSmaObj smaObj = {0};
  memcpy(smaObj.name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  memcpy(smaObj.stb, pStb->name, TSDB_TABLE_FNAME_LEN);
  memcpy(smaObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  smaObj.createdTime = taosGetTimestampMs();
  smaObj.uid = mndGenerateUid(pCreate->name, TSDB_TABLE_FNAME_LEN);
  smaObj.stbUid = pStb->uid;
  smaObj.dbUid = pStb->dbUid;
  smaObj.intervalUnit = pCreate->intervalUnit;
  smaObj.slidingUnit = pCreate->slidingUnit;
  smaObj.timezone = pCreate->timezone;
  smaObj.dstVgId = pCreate->dstVgId;
  smaObj.interval = pCreate->interval;
  smaObj.offset = pCreate->offset;
  smaObj.sliding = pCreate->sliding;
  smaObj.exprLen = pCreate->exprLen;
  smaObj.tagsFilterLen = pCreate->tagsFilterLen;
  smaObj.sqlLen = pCreate->sqlLen;
  smaObj.astLen = pCreate->astLen;

  if (smaObj.exprLen > 0) {
    smaObj.expr = malloc(smaObj.exprLen);
    if (smaObj.expr == NULL) goto _OVER;
    memcpy(smaObj.expr, pCreate->expr, smaObj.exprLen);
  }

  if (smaObj.tagsFilterLen > 0) {
    smaObj.tagsFilter = malloc(smaObj.tagsFilterLen);
    if (smaObj.tagsFilter == NULL) goto _OVER;
    memcpy(smaObj.tagsFilter, pCreate->tagsFilter, smaObj.tagsFilterLen);
  }

  if (smaObj.sqlLen > 0) {
    smaObj.sql = malloc(smaObj.sqlLen);
    if (smaObj.sql == NULL) goto _OVER;
    memcpy(smaObj.sql, pCreate->sql, smaObj.sqlLen);
  }

  if (smaObj.astLen > 0) {
    smaObj.ast = malloc(smaObj.astLen);
    if (smaObj.ast == NULL) goto _OVER;
    memcpy(smaObj.ast, pCreate->ast, smaObj.astLen);
  }

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CREATE_SMA, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to create sma:%s", pTrans->id, pCreate->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetCreateSmaRedoLogs(pMnode, pTrans, &smaObj) != 0) goto _OVER;
  if (mndSetCreateSmaCommitLogs(pMnode, pTrans, &smaObj) != 0) goto _OVER;
  if (mndSetCreateSmaRedoActions(pMnode, pTrans, pDb, &smaObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndCheckCreateSmaReq(SMCreateSmaReq *pCreate) {
  terrno = TSDB_CODE_MND_INVALID_SMA_OPTION;
  if (pCreate->name[0] == 0) return -1;
  if (pCreate->stb[0] == 0) return -1;
  if (pCreate->igExists < 0 || pCreate->igExists > 1) return -1;
  if (pCreate->intervalUnit < 0) return -1;
  if (pCreate->slidingUnit < 0) return -1;
  if (pCreate->timezone < 0) return -1;
  if (pCreate->dstVgId < 0) return -1;
  if (pCreate->interval < 0) return -1;
  if (pCreate->offset < 0) return -1;
  if (pCreate->sliding < 0) return -1;
  if (pCreate->exprLen < 0) return -1;
  if (pCreate->tagsFilterLen < 0) return -1;
  if (pCreate->sqlLen < 0) return -1;
  if (pCreate->astLen < 0) return -1;
  if (pCreate->exprLen != 0 && strlen(pCreate->expr) + 1 != pCreate->exprLen) return -1;
  if (pCreate->tagsFilterLen != 0 && strlen(pCreate->tagsFilter) + 1 != pCreate->tagsFilterLen) return -1;
  if (pCreate->sqlLen != 0 && strlen(pCreate->sql) + 1 != pCreate->sqlLen) return -1;
  if (pCreate->astLen != 0 && strlen(pCreate->ast) + 1 != pCreate->astLen) return -1;

  SName smaName = {0};
  if (tNameFromString(&smaName, pCreate->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE) < 0) return -1;
  if (*(char *)tNameGetTableName(&smaName) == 0) return -1;

  terrno = 0;
  return 0;
}

static int32_t mndProcessMCreateSmaReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SStbObj       *pStb = NULL;
  SSmaObj       *pSma = NULL;
  SDbObj        *pDb = NULL;
  SUserObj      *pUser = NULL;
  SMCreateSmaReq createReq = {0};

  if (tDeserializeSMCreateSmaReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("sma:%s, start to create", createReq.name);
  if (mndCheckCreateSmaReq(&createReq) != 0) {
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.stb);
  if (pStb == NULL) {
    mError("sma:%s, failed to create since stb:%s not exist", createReq.name, createReq.stb);
    goto _OVER;
  }

  pSma = mndAcquireSma(pMnode, createReq.name);
  if (pSma != NULL) {
    if (createReq.igExists) {
      mDebug("sma:%s, already exist in sma:%s, ignore exist is set", createReq.name, pSma->name);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_SMA_ALREADY_EXIST;
      goto _OVER;
    }
  }

  pDb = mndAcquireDbBySma(pMnode, createReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto _OVER;
  }

  code = mndCreateSma(pMnode, pReq, &createReq, pDb, pStb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("sma:%s, failed to create since %s", createReq.name, terrstr());
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseSma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  tFreeSMCreateSmaReq(&createReq);

  return code;
}

static int32_t mndProcessVCreateSmaRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndSetDropSmaRedoLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  SSdbRaw *pRedoRaw = mndSmaActionEncode(pSma);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;

  return 0;
}

static int32_t mndSetDropSmaCommitLogs(SMnode *pMnode, STrans *pTrans, SSmaObj *pSma) {
  SSdbRaw *pCommitRaw = mndSmaActionEncode(pSma);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static int32_t mndSetDropSmaRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SSmaObj *pSma) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropSmaReq(pMnode, pVgroup, pSma, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_SMA;
    action.acceptableCode = TSDB_CODE_VND_SMA_NOT_EXIST;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndDropSma(SMnode *pMnode, SNodeMsg *pReq, SDbObj *pDb, SSmaObj *pSma) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_DROP_SMA, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to drop sma:%s", pTrans->id, pSma->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetDropSmaRedoLogs(pMnode, pTrans, pSma) != 0) goto _OVER;
  if (mndSetDropSmaCommitLogs(pMnode, pTrans, pSma) != 0) goto _OVER;
  if (mndSetDropSmaRedoActions(pMnode, pTrans, pDb, pSma) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessMDropSmaReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SDbObj      *pDb = NULL;
  SSmaObj     *pSma = NULL;
  SMDropSmaReq dropReq = {0};

  if (tDeserializeSMDropSmaReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("sma:%s, start to drop", dropReq.name);

  pSma = mndAcquireSma(pMnode, dropReq.name);
  if (pSma == NULL) {
    if (dropReq.igNotExists) {
      mDebug("sma:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_SMA_NOT_EXIST;
      goto _OVER;
    }
  }

  pDb = mndAcquireDbBySma(pMnode, dropReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto _OVER;
  }

  code = mndDropSma(pMnode, pReq, pDb, pSma);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("sma:%s, failed to drop since %s", dropReq.name, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseSma(pMnode, pSma);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessVDropSmaRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndGetSmaMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_INDEX_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "stb");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_SMA);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveSma(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode  *pMnode = pReq->pNode;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SSmaObj *pSma = NULL;
  int32_t  cols = 0;
  char    *pWrite;
  char     prefix[TSDB_DB_FNAME_LEN] = {0};

  SDbObj *pDb = mndAcquireDb(pMnode, pShow->db);
  if (pDb == NULL) return 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SMA, pShow->pIter, (void **)&pSma);
    if (pShow->pIter == NULL) break;

    if (pSma->dbUid != pDb->uid) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    cols = 0;

    SName smaName = {0};
    tNameFromString(&smaName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, (char *)tNameGetTableName(&smaName));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pSma->createdTime;
    cols++;

    SName stbName = {0};
    tNameFromString(&stbName, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, (char *)tNameGetTableName(&stbName));
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pSma);
  }

  mndReleaseDb(pMnode, pDb);
  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextSma(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
