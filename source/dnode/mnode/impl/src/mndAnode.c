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
#include "tanal.h"
#include "tjson.h"

#ifdef USE_ANAL

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
static int32_t  mndProcessAnalAlgoReq(SRpcMsg *pReq);
static int32_t  mndRetrieveAnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextAnode(SMnode *pMnode, void *pIter);
static int32_t  mndRetrieveAnodesFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextAnodeFull(SMnode *pMnode, void *pIter);
static int32_t  mndGetAnodeAlgoList(const char *url, SAnodeObj *pObj);
static int32_t  mndGetAnodeStatus(SAnodeObj *pObj, char *status, int32_t statusLen);

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
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_ANAL_ALGO, mndProcessAnalAlgoReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ANODE, mndRetrieveAnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ANODE, mndCancelGetNextAnode);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ANODE_FULL, mndRetrieveAnodesFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ANODE_FULL, mndCancelGetNextAnodeFull);

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
  for (int32_t t = 0; t < pObj->numOfAlgos; ++t) {
    SArray *algos = pObj->algos[t];
    for (int32_t a = 0; a < (int32_t)taosArrayGetSize(algos); ++a) {
      SAnodeAlgo *algo = taosArrayGet(algos, a);
      rawDataLen += (2 * sizeof(int32_t) + algo->nameLen);
    }
    rawDataLen += sizeof(int32_t);
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_ANODE, TSDB_ANODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->version, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->urlLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->numOfAlgos, _OVER)
  for (int32_t i = 0; i < pObj->numOfAlgos; ++i) {
    SArray *algos = pObj->algos[i];
    SDB_SET_INT32(pRaw, dataPos, (int32_t)taosArrayGetSize(algos), _OVER)
    for (int32_t j = 0; j < (int32_t)taosArrayGetSize(algos); ++j) {
      SAnodeAlgo *algo = taosArrayGet(algos, j);
      SDB_SET_INT32(pRaw, dataPos, algo->nameLen, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, algo->name, algo->nameLen, _OVER)
      SDB_SET_INT32(pRaw, dataPos, 0, _OVER)  // reserved
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

  if (pObj->urlLen > 0) {
    pObj->url = taosMemoryCalloc(pObj->urlLen, 1);
    if (pObj->url == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->numOfAlgos, _OVER)
  if (pObj->numOfAlgos > 0) {
    pObj->algos = taosMemoryCalloc(pObj->numOfAlgos, sizeof(SArray *));
    if (pObj->algos == NULL) {
      goto _OVER;
    }
  }

  for (int32_t i = 0; i < pObj->numOfAlgos; ++i) {
    int32_t numOfAlgos = 0;
    SDB_GET_INT32(pRaw, dataPos, &numOfAlgos, _OVER)

    pObj->algos[i] = taosArrayInit(2, sizeof(SAnodeAlgo));
    if (pObj->algos[i] == NULL) goto _OVER;

    for (int32_t j = 0; j < numOfAlgos; ++j) {
      SAnodeAlgo algoObj = {0};
      int32_t    reserved = 0;

      SDB_GET_INT32(pRaw, dataPos, &algoObj.nameLen, _OVER)
      if (algoObj.nameLen > 0) {
        algoObj.name = taosMemoryCalloc(algoObj.nameLen, 1);
        if (algoObj.name == NULL) goto _OVER;
      }

      SDB_GET_BINARY(pRaw, dataPos, algoObj.name, algoObj.nameLen, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &reserved, _OVER);

      if (taosArrayPush(pObj->algos[i], &algoObj) == NULL) goto _OVER;
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
  for (int32_t i = 0; i < pObj->numOfAlgos; ++i) {
    SArray *algos = pObj->algos[i];
    for (int32_t j = 0; j < (int32_t)taosArrayGetSize(algos); ++j) {
      SAnodeAlgo *algo = taosArrayGet(algos, j);
      taosMemoryFreeClear(algo->name);
    }
    taosArrayDestroy(algos);
  }
  taosMemoryFreeClear(pObj->algos);
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
  int32_t numOfAlgos = pNew->numOfAlgos;
  void   *algos = pNew->algos;
  pNew->numOfAlgos = pOld->numOfAlgos;
  pNew->algos = pOld->algos;
  pOld->numOfAlgos = numOfAlgos;
  pOld->algos = algos;
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
  if (anodeObj.urlLen > TSDB_ANAL_ANODE_URL_LEN) {
    code = TSDB_CODE_MND_ANODE_TOO_LONG_URL;
    goto _OVER;
  }

  anodeObj.url = taosMemoryCalloc(1, pCreate->urlLen);
  if (anodeObj.url == NULL) goto _OVER;
  (void)memcpy(anodeObj.url, pCreate->url, pCreate->urlLen);

  code = mndGetAnodeAlgoList(anodeObj.url, &anodeObj);
  if (code != 0) goto _OVER;

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
  }

  mndReleaseAnode(pMnode, pObj);
  tFreeSMCreateAnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndUpdateAnode(SMnode *pMnode, SAnodeObj *pAnode, SRpcMsg *pReq) {
  mInfo("anode:%d, start to update", pAnode->id);
  int32_t   code = -1;
  STrans   *pTrans = NULL;
  SAnodeObj anodeObj = {0};
  anodeObj.id = pAnode->id;
  anodeObj.updateTime = taosGetTimestampMs();

  code = mndGetAnodeAlgoList(pAnode->url, &anodeObj);
  if (code != 0) goto _OVER;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-anode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update anode:%d", pTrans->id, anodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateAnodeCommitLogs(pTrans, &anodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndFreeAnode(&anodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndUpdateAllAnodes(SMnode *pMnode, SRpcMsg *pReq) {
  mInfo("update all anodes");
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;
  int32_t rows = 0;
  int32_t numOfRows = sdbGetSize(pSdb, SDB_ANODE);

  void *pIter = NULL;
  while (1) {
    SAnodeObj *pObj = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_ANODE, pIter, (void **)&pObj, &objStatus, true);
    if (pIter == NULL) break;

    rows++;
    void *transReq = NULL;
    if (rows == numOfRows) transReq = pReq;
    code = mndUpdateAnode(pMnode, pObj, transReq);
    sdbRelease(pSdb, pObj);

    if (code != 0) break;
  }

  if (code == 0 && rows == numOfRows) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

static int32_t mndProcessUpdateAnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SAnodeObj       *pObj = NULL;
  SMUpdateAnodeReq updateReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMUpdateAnodeReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_UPDATE_ANODE), NULL, _OVER);

  if (updateReq.anodeId == -1) {
    code = mndUpdateAllAnodes(pMnode, pReq);
  } else {
    pObj = mndAcquireAnode(pMnode, updateReq.anodeId);
    if (pObj == NULL) {
      code = TSDB_CODE_MND_ANODE_NOT_EXIST;
      goto _OVER;
    }
    code = mndUpdateAnode(pMnode, pObj, pReq);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (updateReq.anodeId != -1) {
      mError("anode:%d, failed to update since %s", updateReq.anodeId, tstrerror(code));
    }
  }

  mndReleaseAnode(pMnode, pObj);
  tFreeSMUpdateAnodeReq(&updateReq);
  TAOS_RETURN(code);
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
  char       buf[TSDB_ANAL_ANODE_URL_LEN + VARSTR_HEADER_SIZE];
  char       status[64];
  int32_t    code = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ANODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    if (code != 0) goto _end;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->url, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    status[0] = 0;
    if (mndGetAnodeStatus(pObj, status, 64) == 0) {
      STR_TO_VARSTR(buf, status);
    } else {
      STR_TO_VARSTR(buf, "offline");
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
    if (code != 0) goto _end;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);
    if (code != 0) goto _end;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->updateTime, false);
    if (code != 0) goto _end;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextAnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ANODE);
}

static int32_t mndRetrieveAnodesFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SAnodeObj *pObj = NULL;
  char       buf[TSDB_ANAL_ALGO_NAME_LEN + VARSTR_HEADER_SIZE];
  int32_t    code = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ANODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    for (int32_t t = 0; t < pObj->numOfAlgos; ++t) {
      SArray *algos = pObj->algos[t];

      for (int32_t a = 0; a < taosArrayGetSize(algos); ++a) {
        SAnodeAlgo *algo = taosArrayGet(algos, a);

        cols = 0;
        SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
        if (code != 0) goto _end;

        STR_TO_VARSTR(buf, taosAnalAlgoStr(t));
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        code = colDataSetVal(pColInfo, numOfRows, buf, false);
        if (code != 0) goto _end;

        STR_TO_VARSTR(buf, algo->name);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        code = colDataSetVal(pColInfo, numOfRows, buf, false);
        if (code != 0) goto _end;

        numOfRows++;
      }
    }

    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextAnodeFull(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ANODE);
}

static int32_t mndDecodeAlgoList(SJson *pJson, SAnodeObj *pObj) {
  int32_t code = 0;
  int32_t protocol = 0;
  double  tmp = 0;
  char    buf[TSDB_ANAL_ALGO_NAME_LEN + 1] = {0};

  code = tjsonGetDoubleValue(pJson, "protocol", &tmp);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  protocol = (int32_t)(tmp * 1000);
  if (protocol != 100 && protocol != 1000) return TSDB_CODE_MND_ANODE_INVALID_PROTOCOL;

  code = tjsonGetDoubleValue(pJson, "version", &tmp);
  pObj->version = (int32_t)(tmp * 1000);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  if (pObj->version <= 0) return TSDB_CODE_MND_ANODE_INVALID_VERSION;

  SJson *details = tjsonGetObjectItem(pJson, "details");
  if (details == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t numOfDetails = tjsonGetArraySize(details);

  pObj->algos = taosMemoryCalloc(ANAL_ALGO_TYPE_END, sizeof(SArray *));
  if (pObj->algos == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  pObj->numOfAlgos = ANAL_ALGO_TYPE_END;
  for (int32_t i = 0; i < ANAL_ALGO_TYPE_END; ++i) {
    pObj->algos[i] = taosArrayInit(4, sizeof(SAnodeAlgo));
    if (pObj->algos[i] == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t d = 0; d < numOfDetails; ++d) {
    SJson *detail = tjsonGetArrayItem(details, d);
    if (detail == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    code = tjsonGetStringValue2(detail, "type", buf, sizeof(buf));
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
    EAnalAlgoType type = taosAnalAlgoInt(buf);
    if (type < 0 || type >= ANAL_ALGO_TYPE_END) return TSDB_CODE_MND_ANODE_INVALID_ALGO_TYPE;

    SJson *algos = tjsonGetObjectItem(detail, "algo");
    if (algos == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
    int32_t numOfAlgos = tjsonGetArraySize(algos);
    for (int32_t a = 0; a < numOfAlgos; ++a) {
      SJson *algo = tjsonGetArrayItem(algos, a);
      if (algo == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

      code = tjsonGetStringValue2(algo, "name", buf, sizeof(buf));
      if (code < 0) return TSDB_CODE_MND_ANODE_TOO_LONG_ALGO_NAME;

      SAnodeAlgo algoObj = {0};
      algoObj.nameLen = strlen(buf) + 1;
      if (algoObj.nameLen <= 1) return TSDB_CODE_INVALID_JSON_FORMAT;
      algoObj.name = taosMemoryCalloc(algoObj.nameLen, 1);
      tstrncpy(algoObj.name, buf, algoObj.nameLen);

      if (taosArrayPush(pObj->algos[type], &algoObj) == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

static int32_t mndGetAnodeAlgoList(const char *url, SAnodeObj *pObj) {
  char anodeUrl[TSDB_ANAL_ANODE_URL_LEN + 1] = {0};
  snprintf(anodeUrl, TSDB_ANAL_ANODE_URL_LEN, "%s/%s", url, "list");

  SJson *pJson = taosAnalSendReqRetJson(anodeUrl, ANAL_HTTP_TYPE_GET, NULL);
  if (pJson == NULL) return terrno;

  int32_t code = mndDecodeAlgoList(pJson, pObj);
  if (pJson != NULL) tjsonDelete(pJson);

  TAOS_RETURN(code);
}

static int32_t mndGetAnodeStatus(SAnodeObj *pObj, char *status, int32_t statusLen) {
  int32_t code = 0;
  int32_t protocol = 0;
  double  tmp = 0;
  char    anodeUrl[TSDB_ANAL_ANODE_URL_LEN + 1] = {0};
  snprintf(anodeUrl, TSDB_ANAL_ANODE_URL_LEN, "%s/%s", pObj->url, "status");

  SJson *pJson = taosAnalSendReqRetJson(anodeUrl, ANAL_HTTP_TYPE_GET, NULL);
  if (pJson == NULL) return terrno;

  code = tjsonGetDoubleValue(pJson, "protocol", &tmp);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  protocol = (int32_t)(tmp * 1000);
  if (protocol != 100 && protocol != 1000) {
    code = TSDB_CODE_MND_ANODE_INVALID_PROTOCOL;
    goto _OVER;
  }

  code = tjsonGetStringValue2(pJson, "status", status, statusLen);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  if (strlen(status) == 0) {
    code = TSDB_CODE_MND_ANODE_INVALID_PROTOCOL;
    goto _OVER;
  }

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  TAOS_RETURN(code);
}

static int32_t mndProcessAnalAlgoReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  SSdb                *pSdb = pMnode->pSdb;
  int32_t              code = -1;
  SAnodeObj           *pObj = NULL;
  SAnalUrl             url;
  int32_t              nameLen;
  char                 name[TSDB_ANAL_ALGO_KEY_LEN];
  SRetrieveAnalAlgoReq req = {0};
  SRetrieveAnalAlgoRsp rsp = {0};

  TAOS_CHECK_GOTO(tDeserializeRetrieveAnalAlgoReq(pReq->pCont, pReq->contLen, &req), NULL, _OVER);

  rsp.ver = sdbGetTableVer(pSdb, SDB_ANODE);
  if (req.analVer != rsp.ver) {
    mInfo("dnode:%d, update analysis old ver:%" PRId64 " to new ver:%" PRId64, req.dnodeId, req.analVer, rsp.ver);
    rsp.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
    if (rsp.hash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    void *pIter = NULL;
    while (1) {
      SAnodeObj *pAnode = NULL;
      pIter = sdbFetch(pSdb, SDB_ANODE, pIter, (void **)&pAnode);
      if (pIter == NULL) break;

      url.anode = pAnode->id;
      for (int32_t t = 0; t < pAnode->numOfAlgos; ++t) {
        SArray *algos = pAnode->algos[t];
        url.type = t;

        for (int32_t a = 0; a < taosArrayGetSize(algos); ++a) {
          SAnodeAlgo *algo = taosArrayGet(algos, a);
          nameLen = 1 + snprintf(name, sizeof(name) - 1, "%d:%s", url.type, algo->name);

          SAnalUrl *pOldUrl = taosHashAcquire(rsp.hash, name, nameLen);
          if (pOldUrl == NULL || (pOldUrl != NULL && pOldUrl->anode < url.anode)) {
            if (pOldUrl != NULL) {
              taosMemoryFreeClear(pOldUrl->url);
              if (taosHashRemove(rsp.hash, name, nameLen) != 0) {
                sdbRelease(pSdb, pAnode);
                goto _OVER;
              }
            }
            url.url = taosMemoryMalloc(TSDB_ANAL_ANODE_URL_LEN + TSDB_ANAL_ALGO_TYPE_LEN + 1);
            if (url.url == NULL) {
              sdbRelease(pSdb, pAnode);
              goto _OVER;
            }

            url.urlLen = 1 + snprintf(url.url, TSDB_ANAL_ANODE_URL_LEN + TSDB_ANAL_ALGO_TYPE_LEN, "%s/%s", pAnode->url,
                                      taosAnalAlgoUrlStr(url.type));
            if (taosHashPut(rsp.hash, name, nameLen, &url, sizeof(SAnalUrl)) != 0) {
              taosMemoryFree(url.url);
              sdbRelease(pSdb, pAnode);
              goto _OVER;
            }
          }
        }

        sdbRelease(pSdb, pAnode);
      }
    }
  }

  int32_t contLen = tSerializeRetrieveAnalAlgoRsp(NULL, 0, &rsp);
  void   *pHead = rpcMallocCont(contLen);
  (void)tSerializeRetrieveAnalAlgoRsp(pHead, contLen, &rsp);

  pReq->info.rspLen = contLen;
  pReq->info.rsp = pHead;

_OVER:
  tFreeRetrieveAnalAlgoRsp(&rsp);
  TAOS_RETURN(code);
}

#else

static int32_t mndProcessUnsupportReq(SRpcMsg *pReq) { return TSDB_CODE_OPS_NOT_SUPPORT; }
static int32_t mndRetrieveUnsupport(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t mndInitAnode(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ANODE, mndProcessUnsupportReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_ANODE, mndProcessUnsupportReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ANODE, mndProcessUnsupportReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_ANAL_ALGO, mndProcessUnsupportReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ANODE, mndRetrieveUnsupport);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ANODE_FULL, mndRetrieveUnsupport);
  return 0;
}

void mndCleanupAnode(SMnode *pMnode) {}

#endif