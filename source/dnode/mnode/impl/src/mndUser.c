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
#include "mndUser.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "tkey.h"

#define SDB_USER_VER 1

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRaw *mndUserActionEncode(SUserObj *pUser);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pSrcUser, SUserObj *pDstUser);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, char *user, char *pass, SMnodeMsg *pMsg);
static int32_t  mndProcessCreateUserMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterUserMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropUserMsg(SMnodeMsg *pMsg);

int32_t mndInitUser(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_USER,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
                     .encodeFp = (SdbEncodeFp)mndUserActionEncode,
                     .decodeFp = (SdbDecodeFp)mndUserActionDecode,
                     .insertFp = (SdbInsertFp)mndUserActionInsert,
                     .updateFp = (SdbUpdateFp)mndUserActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndUserActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_USER, mndProcessCreateUserMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_USER, mndProcessAlterUserMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_USER, mndProcessDropUserMsg);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupUser(SMnode *pMnode) {}

static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.readAuth = 1;
  userObj.writeAuth = 1;

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superAuth = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mTrace("user:%s, will be created while deploy sdb", userObj.user);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, "_" TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  return 0;
}

static SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, SDB_USER_VER, sizeof(SUserObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_KEY_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime)
  SDB_SET_INT8(pRaw, dataPos, pUser->superAuth)
  SDB_SET_INT8(pRaw, dataPos, pUser->readAuth)
  SDB_SET_INT8(pRaw, dataPos, pUser->writeAuth)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_USER_VER) {
    mError("failed to decode user since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow  *pRow = sdbAllocRow(sizeof(SUserObj));
  SUserObj *pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->user, TSDB_USER_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->pass, TSDB_KEY_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->acct, TSDB_USER_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pUser->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pUser->updateTime)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pUser->superAuth)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pUser->readAuth)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pUser->writeAuth)

  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform insert action", pUser->user);
  pUser->prohibitDbHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->prohibitDbHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("user:%s, failed to perform insert action since %s", pUser->user, terrstr());
    return -1;
  }

  SAcctObj *pAcct = sdbAcquire(pSdb, SDB_ACCT, pUser->acct);
  if (pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    mError("user:%s, failed to perform insert action since %s", pUser->user, terrstr());
    return -1;
  }
  pUser->acctId = pAcct->acctId;
  sdbRelease(pSdb, pAcct);

  return 0;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action", pUser->user);
  if (pUser->prohibitDbHash) {
    taosHashCleanup(pUser->prohibitDbHash);
    pUser->prohibitDbHash = NULL;
  }

  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pSrcUser, SUserObj *pDstUser) {
  mTrace("user:%s, perform update action", pSrcUser->user);
  memcpy(pSrcUser->user, pDstUser->user, TSDB_USER_LEN);
  memcpy(pSrcUser->pass, pDstUser->pass, TSDB_KEY_LEN);
  memcpy(pSrcUser->acct, pDstUser->acct, TSDB_USER_LEN);
  pSrcUser->createdTime = pDstUser->createdTime;
  pSrcUser->updateTime = pDstUser->updateTime;
  pSrcUser->superAuth = pDstUser->superAuth;
  pSrcUser->readAuth = pDstUser->readAuth;
  pSrcUser->writeAuth = pDstUser->writeAuth;
  return 0;
}

SUserObj *mndAcquireUser(SMnode *pMnode, char *userName) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_USER, userName);
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, char *user, char *pass, SMnodeMsg *pMsg) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superAuth = 0;
  userObj.readAuth = 1;
  userObj.writeAuth = 1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) return -1;

  SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("failed to append redo log since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING);

  SSdbRaw *pUndoRaw = mndUserActionEncode(&userObj);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("failed to append undo log since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("failed to append commit log since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pTrans, mndSyncPropose) != 0) {
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateUserMsg(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  SCreateUserMsg *pCreate = pMsg->rpcMsg.pCont;

  if (pCreate->user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  if (pCreate->pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  SUserObj *pUser = sdbAcquire(pMnode->pSdb, SDB_USER, pCreate->user);
  if (pUser != NULL) {
    sdbRelease(pMnode->pSdb, pUser);
    terrno = TSDB_CODE_MND_USER_ALREADY_EXIST;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  SUserObj *pOperUser = sdbAcquire(pMnode->pSdb, SDB_USER, pMsg->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  int32_t code = mndCreateUser(pMnode, pOperUser->acct, pCreate->user, pCreate->pass, pMsg);
  sdbRelease(pMnode->pSdb, pOperUser);

  if (code != 0) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessAlterUserMsg(SMnodeMsg *pMsg) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process alter user msg since %s", terrstr());
  return -1;
}

static int32_t mndProcessDropUserMsg(SMnodeMsg *pMsg) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process drop user msg since %s", terrstr());
  return -1;
}