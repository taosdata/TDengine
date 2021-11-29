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
#include "mndSync.h"
#include "tkey.h"
#include "mndTrans.h"

#define SDB_USER_VER 1

static SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, SDB_USER_VER, sizeof(SAcctObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_KEY_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime)
  SDB_SET_INT8(pRaw, dataPos, pUser->rootAuth)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_USER_VER) {
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
  SDB_GET_INT8(pRaw, pRow, dataPos, &pUser->rootAuth)

  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  pUser->prohibitDbHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->prohibitDbHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pUser->pAcct = sdbAcquire(pSdb, SDB_ACCT, pUser->acct);
  if (pUser->pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    return -1;
  }

  return 0;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  if (pUser->prohibitDbHash) {
    taosHashCleanup(pUser->prohibitDbHash);
    pUser->prohibitDbHash = NULL;
  }

  if (pUser->acct != NULL) {
    sdbRelease(pSdb, pUser->pAcct);
    pUser->pAcct = NULL;
  }

  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pSrcUser, SUserObj *pDstUser) {
  SUserObj tObj;
  int32_t  len = (int32_t)((int8_t *)tObj.prohibitDbHash - (int8_t *)&tObj);
  memcpy(pDstUser, pSrcUser, len);
  return 0;
}

static int32_t mndCreateDefaultUser(SSdb *pSdb, char *acct, char *user, char *pass) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.rootAuth = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  return sdbWrite(pSdb, pRaw);
}

static int32_t mndCreateDefaultUsers(SSdb *pSdb) {
  if (mndCreateDefaultUser(pSdb, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  if (mndCreateDefaultUser(pSdb, TSDB_DEFAULT_USER, "_" TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  return 0;
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, char *user, char *pass, SMnodeMsg *pMsg) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.rootAuth = 0;

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

static int32_t mndProcessCreateUserMsg(SMnode *pMnode, SMnodeMsg *pMsg) {
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

  SUserObj *pOperUser = sdbAcquire(pMnode->pSdb, SDB_USER, pMsg->conn.user);
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

int32_t mndInitUser(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_USER,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
                     .encodeFp = (SdbEncodeFp)mndUserActionEncode,
                     .decodeFp = (SdbDecodeFp)mndUserActionDecode,
                     .insertFp = (SdbInsertFp)mndUserActionInsert,
                     .updateFp = (SdbUpdateFp)mndUserActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndUserActionDelete};
  sdbSetTable(pMnode->pSdb, table);

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_USER, mndProcessCreateUserMsg);

  return 0;
}

void mndCleanupUser(SMnode *pMnode) {}