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
#include "mndAnode.h"
#include "audit.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define TSDB_ANODE_VER_NUMBER   1
#define TSDB_ANODE_RESERVE_SIZE 64

static SSdbRaw *mndAnodeActionEncode(SAnodeObj *pObj);
static SSdbRow *mndAnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndAnodeActionInsert(SSdb *pSdb, SAnodeObj *pObj);
static int32_t  mndAnodeActionUpdate(SSdb *pSdb, SAnodeObj *pOld, SAnodeObj *pNew);
static int32_t  mndAnodeActionDelete(SSdb *pSdb, SAnodeObj *pObj);
static int32_t  mndProcessCreateAnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessUpdateAnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessDropAnodeReq(SRpcMsg *pReq);
static int32_t  mndRetrieveAnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextAnode(SMnode *pMnode, void *pIter);

int32_t mndInitAnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_ANODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndAnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndAnodeActionDecode,
      .insertFp = (SdbInsertFp)mndAnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndAnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndAnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ANODE, mndProcessCreateAnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_ANODE, mndProcessUpdateAnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ANODE, mndProcessDropAnodeReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ANODE, mndRetrieveAnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ANODE, mndCancelGetNextAnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupAnode(SMnode *pMnode) {}

SAnodeObj *mndAcquireAnode(SMnode *pMnode, int32_t anodeId) {
  SAnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_ANODE, &anodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_ANODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseAnode(SMnode *pMnode, SAnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndAnodeActionEncode(SAnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(SAnodeObj) + TSDB_ANODE_RESERVE_SIZE + pObj->urlLen;
  for (int32_t i = 0; i < pObj->numOfFuncs; ++i) {
    SAnodeFunc *pFunc = &pObj->pFuncs[i];
    rawDataLen += 4;
    rawDataLen += pFunc->nameLen;
    rawDataLen += 4;
    rawDataLen += 4 * pFunc->typeLen;
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_ANODE, TSDB_ANODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->version, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->urlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->numOfFuncs, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  for (int32_t i = 0; i < pObj->numOfFuncs; ++i) {
    SAnodeFunc *pFunc = &pObj->pFuncs[i];
    SDB_SET_INT32(pRaw, dataPos, pFunc->nameLen, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pFunc->typeLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, pFunc->name, pFunc->nameLen, _OVER)
    for (int32_t j = 0; j < pFunc->typeLen; ++j) {
      SDB_SET_INT32(pRaw, dataPos, pFunc->types[j], _OVER)
    }
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_ANODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("anode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("anode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndAnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SAnodeObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_ANODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SAnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->version, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->urlLen, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->numOfFuncs, _OVER)

  if (pObj->urlLen > 0) {
    pObj->url = taosMemoryCalloc(pObj->urlLen, 1);
    if (pObj->url == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen , _OVER)
  }

  if (pObj->numOfFuncs > 0) {
    pObj->pFuncs = taosMemoryCalloc(pObj->numOfFuncs, sizeof(SAnodeFunc));
    if (pObj->pFuncs == NULL) {
      goto _OVER;
    }
  }

  for (int32_t i = 0; i < pObj->numOfFuncs; ++i) {
    SAnodeFunc *pFunc = &pObj->pFuncs[i];
    SDB_GET_INT32(pRaw, dataPos, &pFunc->nameLen, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &pFunc->typeLen, _OVER)
    if (pFunc->nameLen > 0) {
      pFunc->name = taosMemoryCalloc(pFunc->nameLen, 1);
    }
    if (pFunc->typeLen > 0) {
      pFunc->types = taosMemoryCalloc(pFunc->typeLen, sizeof(int32_t));
    }
    if (pFunc->name == NULL || pFunc->types == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pFunc->name, pFunc->nameLen, _OVER)
    for (int32_t j = 0; j < pFunc->typeLen; ++j) {
      SDB_GET_INT32(pRaw, dataPos, &pFunc->types[j], _OVER)
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_ANODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("anode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->url);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("anode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static void mndFreeAnode(SAnodeObj *pObj) {
  taosMemoryFreeClear(pObj->url);
  for (int32_t i = 0; i < pObj->numOfFuncs; ++i) {
    SAnodeFunc *pFunc = &pObj->pFuncs[i];
    taosMemoryFreeClear(pFunc->name);
    taosMemoryFreeClear(pFunc->types);
  }
  taosMemoryFreeClear(pObj->pFuncs);
}

static int32_t mndAnodeActionInsert(SSdb *pSdb, SAnodeObj *pObj) {
  mTrace("anode:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndAnodeActionDelete(SSdb *pSdb, SAnodeObj *pObj) {
  mTrace("anode:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeAnode(pObj);
  return 0;
}

static int32_t mndAnodeActionUpdate(SSdb *pSdb, SAnodeObj *pOld, SAnodeObj *pNew) {
  mTrace("anode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  int32_t numOfFuncs = pNew->numOfFuncs;
  void   *pFuncs = pNew->pFuncs;
  pNew->numOfFuncs = pOld->numOfFuncs;
  pNew->pFuncs = pOld->pFuncs;
  pOld->numOfFuncs = numOfFuncs;
  pOld->pFuncs = pFuncs;
  pOld->updateTime = pNew->updateTime;
  pOld->version = pNew->version;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

static int32_t mndSetCreateAnodeRedoLogs(STrans *pTrans, SAnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndAnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateAnodeUndoLogs(STrans *pTrans, SAnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndAnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateAnodeCommitLogs(STrans *pTrans, SAnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndAnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndCreateAnode(SMnode *pMnode, SRpcMsg *pReq, SMCreateAnodeReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SAnodeObj anodeObj = {0};
  anodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_ANODE);

  anodeObj.createdTime = taosGetTimestampMs();
  anodeObj.updateTime = anodeObj.createdTime;
  anodeObj.version = 0;
  anodeObj.urlLen = pCreate->urlLen;
  anodeObj.url = taosMemoryCalloc(1, pCreate->urlLen);

  if (anodeObj.url == NULL) goto _OVER;
  memcpy(anodeObj.url, pCreate->url, pCreate->urlLen);

  // get from restful, test begin
  anodeObj.numOfFuncs = 2;

  anodeObj.pFuncs = taosMemoryCalloc(anodeObj.numOfFuncs, sizeof(SAnodeFunc));
  SAnodeFunc *pFunc1 = &anodeObj.pFuncs[0];
  pFunc1->typeLen = 3;
  pFunc1->nameLen = strlen("arima") + 1;
  pFunc1->name = taosMemoryCalloc(pFunc1->nameLen, 1);
  pFunc1->types = taosMemoryCalloc(pFunc1->typeLen, sizeof(int32_t));
  tstrncpy(pFunc1->name, "arima", pFunc1->nameLen);
  pFunc1->types[0] = AFUNC_TYPE_ANOMALY_WINDOW;
  pFunc1->types[1] = AFUNC_TYPE_FORECAST;
  pFunc1->types[2] = AFUNC_TYPE_ANOMALY_DETECT;

  SAnodeFunc *pFunc2 = &anodeObj.pFuncs[1];
  pFunc2->typeLen = 2;
  pFunc2->nameLen = strlen("ar") + 1;
  pFunc2->name = taosMemoryCalloc(pFunc2->nameLen, 1);
  pFunc2->types = taosMemoryCalloc(pFunc2->typeLen, sizeof(int32_t));
  tstrncpy(pFunc2->name, "ar", pFunc2->nameLen);
  pFunc2->types[0] = AFUNC_TYPE_ANOMALY_WINDOW;
  pFunc2->types[1] = AFUNC_TYPE_FORECAST;
  // test end

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-anode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create anode:%s as anode:%d", pTrans->id, pCreate->url, anodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateAnodeRedoLogs(pTrans, &anodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateAnodeUndoLogs(pTrans, &anodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateAnodeCommitLogs(pTrans, &anodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndFreeAnode(&anodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static SAnodeObj *mndAcquireAnodeByURL(SMnode *pMnode, char *url) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SAnodeObj *pAnode = NULL;
    pIter = sdbFetch(pSdb, SDB_ANODE, pIter, (void **)&pAnode);
    if (pIter == NULL) break;

    if (strcasecmp(url, pAnode->url) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pAnode;
    }

    sdbRelease(pSdb, pAnode);
  }

  terrno = TSDB_CODE_MND_ANODE_NOT_EXIST;
  return NULL;
}

static int32_t mndProcessCreateAnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SAnodeObj       *pObj = NULL;
  SMCreateAnodeReq createReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMCreateAnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("anode:%s, start to create", createReq.url);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_ANODE), NULL, _OVER);

  pObj = mndAcquireAnodeByURL(pMnode, createReq.url);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_ANODE_ALREADY_EXIST;
    goto _OVER;
  }

  code = mndCreateAnode(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("anode:%s, failed to create since %s", createReq.url, tstrerror(code));
    TAOS_RETURN(code);
  }

  mndReleaseAnode(pMnode, pObj);
  tFreeSMCreateAnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateAnodeReq(SRpcMsg *pReq) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

static int32_t mndSetDropAnodeRedoLogs(STrans *pTrans, SAnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndAnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));
  TAOS_RETURN(code);
}

static int32_t mndSetDropAnodeCommitLogs(STrans *pTrans, SAnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndAnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropAnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SAnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  TAOS_CHECK_RETURN(mndSetDropAnodeRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropAnodeCommitLogs(pTrans, pObj));
  return 0;
}

static int32_t mndDropAnode(SMnode *pMnode, SRpcMsg *pReq, SAnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-anode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to drop anode:%d", pTrans->id, pObj->id);
  TAOS_CHECK_GOTO(mndSetDropAnodeInfoToTrans(pMnode, pTrans, pObj, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropAnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SAnodeObj     *pObj = NULL;
  SMDropAnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropAnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("anode:%d, start to drop", dropReq.anodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_ANODE), NULL, _OVER);

  if (dropReq.anodeId <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireAnode(pMnode, dropReq.anodeId);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  code = mndDropAnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("anode:%d, failed to drop since %s", dropReq.anodeId, tstrerror(code));
  }

  mndReleaseAnode(pMnode, pObj);
  tFreeSMDropAnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveAnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SAnodeObj *pObj = NULL;
  char       buf[TSDB_URL_LEN + VARSTR_HEADER_SIZE];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ANODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->url, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);

    const char *status = "ready";
    // get from anode
    STR_TO_VARSTR(buf, status);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->updateTime, false);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextAnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ANODE);
}
