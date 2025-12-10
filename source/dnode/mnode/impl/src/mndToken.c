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

#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define TOKEN_VER_NUMBER   1
#define TOKEN_RESERVE_SIZE 64


static SSdbRaw *mndTokenActionEncode(STokenObj *pObj) {
  int32_t code = 0, lino = 0;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TOKEN, TOKEN_VER_NUMBER, sizeof(STokenObj) + TOKEN_RESERVE_SIZE);
  if (pRaw == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pObj->name, sizeof(pObj->name), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->user, sizeof(pObj->user), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->provider, sizeof(pObj->provider), _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->extraInfo, sizeof(pObj->extraInfo), _OVER)
  SDB_SET_INT8(pRaw, dataPos, pObj->enabled, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->expireTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TOKEN_RESERVE_SIZE, _OVER)

_OVER:
  terrno = code;
  if (code != 0) {
    mError("token:%s, failed to encode to raw:%p since %s", pObj->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("token:%s, encode to raw:%p, row:%p", pObj->name, pRaw, pObj);
  return pRaw;
}



static SSdbRow *mndTokenActionDecode(SSdbRaw *pRaw) {
  int32_t    code = 0, lino = 0;
  SSdbRow   *pRow = NULL;
  STokenObj *pObj = NULL;
  int8_t     sver = 0;


  if ((code = sdbGetRawSoftVer(pRaw, &sver)) != 0) {
    goto _OVER;
  }

  if (sver != TOKEN_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(STokenObj));
  if (pRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    goto _OVER;
  }

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pObj->name, sizeof(pObj->name), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->user, sizeof(pObj->user), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->provider, sizeof(pObj->provider), _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pObj->extraInfo, sizeof(pObj->extraInfo), _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pObj->enabled, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->expireTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TOKEN_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pObj->lock);

_OVER:
  terrno = code;
  if (code != 0) {
    mError("token:%s, failed to decode from raw:%p since %s", pObj == NULL ? "" : pObj->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("token:%s, decode from raw:%p, row:%p", pObj->name, pRaw, pObj);
  return pRow;
}



static int32_t mndTokenActionInsert(SSdb *pSdb, STokenObj *pObj) {
  mTrace("token:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}



static int32_t mndTokenActionDelete(SSdb *pSdb, STokenObj *pObj) {
  mTrace("token:%s, perform delete action, row:%p", pObj->name, pObj);
  return 0;
}



static int32_t mndTokenActionUpdate(SSdb *pSdb, STokenObj *pOld, STokenObj *pNew) {
  mTrace("token:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  tstrncpy(pOld->provider, pNew->provider, sizeof(pOld->provider));
  tstrncpy(pOld->extraInfo, pNew->extraInfo, sizeof(pOld->extraInfo));
  pOld->enabled    = pNew->enabled;
  pOld->expireTime = pNew->expireTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}



static int32_t mndCreateToken(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SCreateTokenReq *pCreate) {
  int32_t code = -1;

  STokenObj tokenObj = {0};
  tokenObj.createdTime = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-token");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, to create token from:%s", pTrans->id, pCreate->user);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}



static int32_t mndProcessCreateTokenReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  STokenObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SCreateTokenReq createReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSCreateTokenReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("token from user:%s, start to create", createReq.user);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_TOKEN), NULL, _OVER);

  /*
  pObj = mndAcquireToken(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    code = terrno = TSDB_CODE_MND_TOKEN_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_TOKEN_NOT_EXIST) {
    code = terrno;
    goto _OVER;
  }
    */

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to create token from user %s since %s", createReq.user, tstrerror(code));
    TAOS_RETURN(code);
  }

  //  mndReleaseToken(pMnode, pObj);
  tFreeSCreateTokenReq(&createReq);

  TAOS_RETURN(code);
}



static int32_t mndDropToken(SMnode *pMnode, SRpcMsg *pReq, STokenObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-token");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to drop token:%s", pTrans->id, pObj->name);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}



static int32_t mndProcessAlterTokenReq(SRpcMsg *pReq) {

_OVER:
  return 0;
}



static int32_t mndProcessDropTokenReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  STokenObj     *pObj = NULL;
  SDnodeObj     *pDnode = NULL;
  SDropTokenReq  dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropTokenReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("token:%s, start to drop", dropReq.name);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_TOKEN), NULL, _OVER);

  /*
  if (dropReq.id <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }
    */

  //pObj = mndAcquireToken(pMnode, dropReq.id);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  // check deletable
  //code = mndDropToken(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("token:%s, failed to drop since %s", dropReq.name, tstrerror(code));
  }

  //mndReleaseToken(pMnode, pObj);
  tFreeSDropTokenReq(&dropReq);
  TAOS_RETURN(code);
}



static int32_t mndRetrieveTokens(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   numOfRows = 0;
  STokenObj *pToken = NULL;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOKEN, pShow->pIter, (void **)&pToken);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    //STR_WITH_MAXSIZE_TO_VARSTR(name, pToken->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    //COL_DATA_SET_VAL_GOTO((const char *)&pToken->superUser, false, pToken, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    //COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    //COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    // COL_DATA_SET_VAL_GOTO((const char *)&pUser->createdTime, false, pUser, pShow->pIter, _exit);

    numOfRows++;
    sdbRelease(pSdb, pToken);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}



static void mndCancelGetNextToken(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_TOKEN);
}



int32_t mndInitToken(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_BNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndTokenActionEncode,
      .decodeFp = (SdbDecodeFp)mndTokenActionDecode,
      .insertFp = (SdbInsertFp)mndTokenActionInsert,
      .updateFp = (SdbUpdateFp)mndTokenActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTokenActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOKEN, mndProcessCreateTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_TOKEN, mndProcessAlterTokenReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOKEN, mndProcessDropTokenReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndRetrieveTokens);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOKEN, mndCancelGetNextToken);
  return sdbSetTable(pMnode->pSdb, table);
}



void mndCleanupToken(SMnode *pMnode) {
}



int32_t mndAcquireToken(SMnode *pMnode, const char *token, STokenObj **ppToken) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppToken = sdbAcquire(pSdb, SDB_TOKEN, token);
  if (*ppToken == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_TOKEN_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_TOKEN_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseToken(SMnode *pMnode, STokenObj *pToken) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pToken);
}

/*
void mndUpdateUser(SMnode *pMnode, SUserObj *pUser, SRpcMsg *pReq) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to update since %s", pUser->user, terrstr());
    return;
  }
  mInfo("trans:%d, used to update user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return;
  }
  code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  if (code < 0) {
    mndTransDrop(pTrans);
    return;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return;
  }

  mndTransDrop(pTrans);
}


*/