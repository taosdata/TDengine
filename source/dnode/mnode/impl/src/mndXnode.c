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

#include "mndXnode.h"
#include "audit.h"
#include "mndDnode.h"
#include "mndInt.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "taoserror.h"
#include "tjson.h"

#define TSDB_XNODE_VER_NUMBER   1
#define TSDB_XNODE_RESERVE_SIZE 64

static SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj);
static SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj);
static int32_t  mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew);
static int32_t  mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj);
static int32_t  mndProcessCreateXnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessUpdateXnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessDropXnodeReq(SRpcMsg *pReq);
static int32_t  mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextXnode(SMnode *pMnode, void *pIter);
static int32_t  mndRetrieveXnodesFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextXnodeFull(SMnode *pMnode, void *pIter);
static int32_t  mndGetXnodeAlgoList(const char *url, SXnodeObj *pObj);
static int32_t  mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen);

int32_t mndInitXnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_XNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE, mndProcessCreateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE, mndProcessUpdateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE, mndProcessDropXnodeReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE, mndRetrieveXnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE, mndCancelGetNextXnode);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_FULL, mndRetrieveXnodesFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_FULL, mndCancelGetNextXnodeFull);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupXnode(SMnode *pMnode) {}

SXnodeObj *mndAcquireXnode(SMnode *pMnode, int32_t xnodeId) {
  SXnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE, &xnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseXnode(SMnode *pMnode, SXnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(SXnodeObj) + TSDB_XNODE_RESERVE_SIZE + pObj->urlLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->version, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->urlLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->numOfAlgos, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *  pRow = NULL;
  SXnodeObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeObj));
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
    // pObj->algos = taosMemoryCalloc(pObj->numOfAlgos, sizeof(SArray *));
    // if (pObj->algos == NULL) {
    //   goto _OVER;
    // }
  }

  for (int32_t i = 0; i < pObj->numOfAlgos; ++i) {
    int32_t numOfAlgos = 0;
    SDB_GET_INT32(pRaw, dataPos, &numOfAlgos, _OVER)

    // pObj->algos[i] = taosArrayInit(2, sizeof(SXnodeAlgo));
    // if (pObj->algos[i] == NULL) goto _OVER;

    for (int32_t j = 0; j < numOfAlgos; ++j) {
      // SXnodeAlgo algoObj = {0};
      // int32_t    reserved = 0;

      // SDB_GET_INT32(pRaw, dataPos, &algoObj.nameLen, _OVER)
      // if (algoObj.nameLen > 0) {
      //   algoObj.name = taosMemoryCalloc(algoObj.nameLen, 1);
      //   if (algoObj.name == NULL) goto _OVER;
      // }

      // SDB_GET_BINARY(pRaw, dataPos, algoObj.name, algoObj.nameLen, _OVER)
      // SDB_GET_INT32(pRaw, dataPos, &reserved, _OVER);

      // if (taosArrayPush(pObj->algos[i], &algoObj) == NULL) goto _OVER;
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->url);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static void mndFreeXnode(SXnodeObj *pObj) {
  // taosMemoryFreeClear(pObj->url);
  // for (int32_t i = 0; i < pObj->numOfAlgos; ++i) {
  //   SArray *algos = pObj->algos[i];
  //   for (int32_t j = 0; j < (int32_t)taosArrayGetSize(algos); ++j) {
  //     SXnodeAlgo *algo = taosArrayGet(algos, j);
  //     taosMemoryFreeClear(algo->name);
  //   }
  //   taosArrayDestroy(algos);
  // }
  // taosMemoryFreeClear(pObj->algos);
}

static int32_t mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj) {
  mTrace("xnode:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj) {
  mTrace("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnode(pObj);
  return 0;
}

static int32_t mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew) {
  mTrace("xnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  int32_t numOfAlgos = pNew->numOfAlgos;
  // void   *algos = pNew->algos;
  // pNew->numOfAlgos = pOld->numOfAlgos;
  // pNew->algos = pOld->algos;
  // pOld->numOfAlgos = numOfAlgos;
  // pOld->algos = algos;
  pOld->updateTime = pNew->updateTime;
  pOld->version = pNew->version;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

static int32_t mndSetCreateXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeUndoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndCreateXnode(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE);
  xnodeObj.createdTime = taosGetTimestampMs();
  xnodeObj.updateTime = xnodeObj.createdTime;
  xnodeObj.version = 0;
  xnodeObj.urlLen = pCreate->urlLen;
  // if (xnodeObj.urlLen > TSDB_XNODE_URL_LEN) {
  //   code = TSDB_CODE_MND_XNODE_TOO_LONG_URL;
  //   goto _OVER;
  // }

  xnodeObj.url = taosMemoryCalloc(1, pCreate->urlLen);
  if (xnodeObj.url == NULL) goto _OVER;
  (void)memcpy(xnodeObj.url, pCreate->url, pCreate->urlLen);
  mInfo("create xnode, xnode.id:%d, xnode.url: %s, xnode.time:%ld", xnodeObj.id, xnodeObj.url, xnodeObj.createdTime);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mInfo("failed to create transaction for xnode:%s, code:0x%x:%s", pCreate->url, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode:%s as xnode:%d", pTrans->id, pCreate->url, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeRedoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUndoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndFreeXnode(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static SXnodeObj *mndAcquireXnodeByURL(SMnode *pMnode, char *url) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeObj *pXnode = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE, pIter, (void **)&pXnode);
    if (pIter == NULL) break;

    if (strcasecmp(url, pXnode->url) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pXnode;
    }

    sdbRelease(pSdb, pXnode);
  }

  mError("xnode:%s, not found", url);
  // terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  return NULL;
}

static int32_t mndProcessCreateXnodeReq(SRpcMsg *pReq) {
  mInfo("xnode create request received, contLen:%d", pReq->contLen);
  SMnode *         pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj *      pObj = NULL;
  SMCreateXnodeReq createReq = {0};

  if ((code = grantCheck(TSDB_GRANT_TD_GPT)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("xnode:%s, start to create", createReq.url);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_XNODE), NULL, _OVER);

  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  }

  code = mndCreateXnode(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%s, failed to create since %s", createReq.url, tstrerror(code));
  }

  mndReleaseXnode(pMnode, pObj);
  tFreeSMCreateXnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndUpdateXnode(SMnode *pMnode, SXnodeObj *pXnode, SRpcMsg *pReq) {
  mInfo("xnode:%d, start to update", pXnode->id);
  int32_t   code = -1;
  STrans *  pTrans = NULL;
  SXnodeObj xnodeObj = {0};
  xnodeObj.id = pXnode->id;
  xnodeObj.updateTime = taosGetTimestampMs();

  code = mndGetXnodeAlgoList(pXnode->url, &xnodeObj);
  if (code != 0) goto _OVER;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update xnode:%d", pTrans->id, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndFreeXnode(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndUpdateAllXnodes(SMnode *pMnode, SRpcMsg *pReq) {
  mInfo("update all xnodes");
  SSdb *  pSdb = pMnode->pSdb;
  int32_t code = 0;
  int32_t rows = 0;
  int32_t numOfRows = sdbGetSize(pSdb, SDB_XNODE);

  void *pIter = NULL;
  while (1) {
    SXnodeObj *pObj = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_XNODE, pIter, (void **)&pObj, &objStatus, true);
    if (pIter == NULL) break;

    rows++;
    void *transReq = NULL;
    if (rows == numOfRows) transReq = pReq;
    code = mndUpdateXnode(pMnode, pObj, transReq);
    sdbRelease(pSdb, pObj);

    if (code != 0) break;
  }

  if (code == 0 && rows == numOfRows) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq) {
  SMnode *         pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj *      pObj = NULL;
  SMUpdateXnodeReq updateReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_UPDATE_XNODE), NULL, _OVER);

  if (updateReq.xnodeId == -1) {
    code = mndUpdateAllXnodes(pMnode, pReq);
  } else {
    pObj = mndAcquireXnode(pMnode, updateReq.xnodeId);
    if (pObj == NULL) {
      code = TSDB_CODE_MND_XNODE_NOT_EXIST;
      goto _OVER;
    }
    code = mndUpdateXnode(pMnode, pObj, pReq);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (updateReq.xnodeId != -1) {
      mError("xnode:%d, failed to update since %s", updateReq.xnodeId, tstrerror(code));
    }
  }

  mndReleaseXnode(pMnode, pObj);
  tFreeSMUpdateXnodeReq(&updateReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return code;
}

static int32_t mndSetDropXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SXnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  TAOS_CHECK_RETURN(mndSetDropXnodeRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropXnodeCommitLogs(pTrans, pObj));
  return 0;
}

static int32_t mndDropXnode(SMnode *pMnode, SRpcMsg *pReq, SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, to drop xnode:%d", pTrans->id, pObj->id);

  code = mndSetDropXnodeInfoToTrans(pMnode, pTrans, pObj, false);
  mndReleaseXnode(pMnode, pObj);

  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq) {
  SMnode *       pMnode = pReq->info.node;
  int32_t        code = -1;
  SXnodeObj *    pObj = NULL;
  SMDropXnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("xnode:%d, start to drop", dropReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_XNODE), NULL, _OVER);

  if (dropReq.xnodeId <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnode(pMnode, dropReq.xnodeId);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = mndDropXnode(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", dropReq.xnodeId, tstrerror(code));
  }

  tFreeSMDropXnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *   pMnode = pReq->info.node;
  SSdb *     pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SXnodeObj *pObj = NULL;
  char       buf[128 + VARSTR_HEADER_SIZE];
  char       status[64];
  int32_t    code = 0;
  mInfo("show.type:%d, %s:%d: retrieve xnodes with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE, pShow->pIter, (void **)&pObj);
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
    if (mndGetXnodeStatus(pObj, status, 64) == 0) {
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

static void mndCancelGetNextXnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE);
}

static int32_t mndRetrieveXnodesFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *   pMnode = pReq->info.node;
  SSdb *     pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SXnodeObj *pObj = NULL;
  char       buf[TSDB_ANALYTIC_ALGO_NAME_LEN + VARSTR_HEADER_SIZE];
  int32_t    code = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    for (int32_t t = 0; t < pObj->numOfAlgos; ++t) {
      // SArray *algos = pObj->algos[t];

      // for (int32_t a = 0; a < taosArrayGetSize(algos); ++a) {
      //   SXnodeAlgo *algo = taosArrayGet(algos, a);

      //   cols = 0;
      //   SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      //   code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
      //   if (code != 0) goto _end;

      //   STR_TO_VARSTR(buf, taosAnalysisAlgoType(t));
      //   pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      //   code = colDataSetVal(pColInfo, numOfRows, buf, false);
      //   if (code != 0) goto _end;

      //   STR_TO_VARSTR(buf, algo->name);
      //   pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      //   code = colDataSetVal(pColInfo, numOfRows, buf, false);
      //   if (code != 0) goto _end;

      //   numOfRows++;
      // }
    }

    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextXnodeFull(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE);
}

static int32_t mndDecodeAlgoList(SJson *pJson, SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t protocol = 0;
  double  tmp = 0;
  char    buf[TSDB_ANALYTIC_ALGO_NAME_LEN + 1] = {0};

  code = tjsonGetDoubleValue(pJson, "protocol", &tmp);
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  protocol = (int32_t)(tmp * 1000);
  if (protocol != 100 && protocol != 1000) return TSDB_CODE_MND_XNODE_INVALID_PROTOCOL;

  code = tjsonGetDoubleValue(pJson, "version", &tmp);
  pObj->version = (int32_t)(tmp * 1000);
#if 0
  if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  if (pObj->version <= 0) return TSDB_CODE_MND_XNODE_INVALID_VERSION;
#endif

  SJson *details = tjsonGetObjectItem(pJson, "details");
  if (details == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
  int32_t numOfDetails = tjsonGetArraySize(details);

  // pObj->algos = taosMemoryCalloc(ANALY_ALGO_TYPE_END, sizeof(SArray *));
  // if (pObj->algos == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  // pObj->numOfAlgos = ANALY_ALGO_TYPE_END;
  // for (int32_t i = 0; i < ANALY_ALGO_TYPE_END; ++i) {
  //   pObj->algos[i] = taosArrayInit(4, sizeof(SXnodeAlgo));
  //   if (pObj->algos[i] == NULL) return TSDB_CODE_OUT_OF_MEMORY;
  // }

  for (int32_t d = 0; d < numOfDetails; ++d) {
    SJson *detail = tjsonGetArrayItem(details, d);
    if (detail == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    code = tjsonGetStringValue2(detail, "type", buf, sizeof(buf));
    if (code < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
    // EAnalAlgoType type = taosAnalyAlgoInt(buf);
    // if (type < 0 || type >= ANALY_ALGO_TYPE_END) return TSDB_CODE_MND_XNODE_INVALID_ALGO_TYPE;

    SJson *algos = tjsonGetObjectItem(detail, "algo");
    if (algos == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;
    int32_t numOfAlgos = tjsonGetArraySize(algos);
    for (int32_t a = 0; a < numOfAlgos; ++a) {
      SJson *algo = tjsonGetArrayItem(algos, a);
      if (algo == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

      code = tjsonGetStringValue2(algo, "name", buf, sizeof(buf));
      if (code < 0) return TSDB_CODE_MND_XNODE_TOO_LONG_ALGO_NAME;

      // SXnodeAlgo algoObj = {0};
      // algoObj.nameLen = strlen(buf) + 1;
      // if (algoObj.nameLen <= 1) return TSDB_CODE_INVALID_JSON_FORMAT;
      // algoObj.name = taosMemoryCalloc(algoObj.nameLen, 1);
      // tstrncpy(algoObj.name, buf, algoObj.nameLen);

      // if (taosArrayPush(pObj->algos[type], &algoObj) == NULL) return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

SJson *taosGetTasks(const char *url) {
  // This function should send an HTTP request to the given URL and return the JSON response.
  // The implementation is not provided here as it is not part of the original code.
  return NULL;  // Placeholder
}
static int32_t mndGetXnodeAlgoList(const char *url, SXnodeObj *pObj) {
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/%s", url, "list");

  SJson *pJson = taosGetTasks(xnodeUrl);
  if (pJson == NULL) return terrno;

  int32_t code = mndDecodeAlgoList(pJson, pObj);
  if (pJson != NULL) tjsonDelete(pJson);

  TAOS_RETURN(code);
}

static int32_t mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen) {
  int32_t code = 0;
  int32_t protocol = 0;
  double  tmp = 0;
  char    xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/%s", pObj->url, "status");

  SJson *pJson = NULL;  // taosAnalySendReqRetJson(xnodeUrl, ANALYTICS_HTTP_TYPE_GET, NULL, 0);
  if (pJson == NULL) return terrno;

  code = tjsonGetDoubleValue(pJson, "protocol", &tmp);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  protocol = (int32_t)(tmp * 1000);
  if (protocol != 100 && protocol != 1000) {
    code = TSDB_CODE_MND_XNODE_INVALID_PROTOCOL;
    goto _OVER;
  }

  code = tjsonGetStringValue2(pJson, "status", status, statusLen);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  if (strlen(status) == 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_PROTOCOL;
    goto _OVER;
  }

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  TAOS_RETURN(code);
}

static int32_t mndProcessAnalAlgoReq(SRpcMsg *pReq) {
  return 0;
  //   SMnode              *pMnode = pReq->info.node;
  //   SSdb                *pSdb = pMnode->pSdb;
  //   int32_t              code = -1;
  //   SXnodeObj           *pObj = NULL;
  //   SAnalyticsUrl             url;
  //   int32_t              nameLen;
  //   char                 name[TSDB_ANALYTIC_ALGO_KEY_LEN];
  //   SRetrieveAnalyticsAlgoReq req = {0};
  //   SRetrieveAnalyticAlgoRsp rsp = {0};

  //   TAOS_CHECK_GOTO(tDeserializeRetrieveAnalyticAlgoReq(pReq->pCont, pReq->contLen, &req), NULL, _OVER);

  //   rsp.ver = sdbGetTableVer(pSdb, SDB_XNODE);
  //   if (req.analVer != rsp.ver) {
  //     mInfo("dnode:%d, update analysis old ver:%" PRId64 " to new ver:%" PRId64, req.dnodeId, req.analVer, rsp.ver);
  //     rsp.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  //     if (rsp.hash == NULL) {
  //       terrno = TSDB_CODE_OUT_OF_MEMORY;
  //       goto _OVER;
  //     }

  //     void *pIter = NULL;
  //     while (1) {
  //       SXnodeObj *pXnode = NULL;
  //       pIter = sdbFetch(pSdb, SDB_XNODE, pIter, (void **)&pXnode);
  //       if (pIter == NULL) break;

  //       url.xnode = pXnode->id;
  //       for (int32_t t = 0; t < pXnode->numOfAlgos; ++t) {
  //         SArray *algos = pXnode->algos[t];
  //         url.type = t;

  //         for (int32_t a = 0; a < taosArrayGetSize(algos); ++a) {
  //           SXnodeAlgo *algo = taosArrayGet(algos, a);
  //           nameLen = 1 + tsnprintf(name, sizeof(name) - 1, "%d:%s", url.type, algo->name);

  //           SAnalyticsUrl *pOldUrl = taosHashAcquire(rsp.hash, name, nameLen);
  //           if (pOldUrl == NULL || (pOldUrl != NULL && pOldUrl->xnode < url.xnode)) {
  //             if (pOldUrl != NULL) {
  //               taosMemoryFreeClear(pOldUrl->url);
  //               if (taosHashRemove(rsp.hash, name, nameLen) != 0) {
  //                 sdbRelease(pSdb, pXnode);
  //                 goto _OVER;
  //               }
  //             }
  //             url.url = taosMemoryMalloc(TSDB_XNODE_URL_LEN + TSDB_ANALYTIC_ALGO_TYPE_LEN + 1);
  //             if (url.url == NULL) {
  //               sdbRelease(pSdb, pXnode);
  //               goto _OVER;
  //             }

  //             url.urlLen = 1 + tsnprintf(url.url, TSDB_XNODE_URL_LEN + TSDB_ANALYTIC_ALGO_TYPE_LEN, "%s/%s",
  //             pXnode->url,
  //                                       taosAnalyAlgoUrlStr(url.type));
  //             if (taosHashPut(rsp.hash, name, nameLen, &url, sizeof(SAnalyticsUrl)) != 0) {
  //               taosMemoryFree(url.url);
  //               sdbRelease(pSdb, pXnode);
  //               goto _OVER;
  //             }
  //           }
  //         }
  //       }

  //       sdbRelease(pSdb, pXnode);
  //     }
  //   }

  //   int32_t contLen = tSerializeRetrieveAnalyticAlgoRsp(NULL, 0, &rsp);
  //   void   *pHead = rpcMallocCont(contLen);
  //   (void)tSerializeRetrieveAnalyticAlgoRsp(pHead, contLen, &rsp);

  //   pReq->info.rspLen = contLen;
  //   pReq->info.rsp = pHead;

  // _OVER:
  //   tFreeRetrieveAnalyticAlgoRsp(&rsp);
  //   TAOS_RETURN(code);
}
