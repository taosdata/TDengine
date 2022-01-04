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

#define TSDB_USER_VER_NUMBER 1
#define TSDB_USER_RESERVE_SIZE 64

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRaw *mndUserActionEncode(SUserObj *pUser);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOldUser, SUserObj *pNewUser);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, char *user, char *pass, SMnodeMsg *pMsg);
static int32_t  mndProcessCreateUserMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterUserMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropUserMsg(SMnodeMsg *pMsg);
static int32_t  mndGetUserMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveUsers(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);

int32_t mndInitUser(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_USER,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
                     .encodeFp = (SdbEncodeFp)mndUserActionEncode,
                     .decodeFp = (SdbDecodeFp)mndUserActionDecode,
                     .insertFp = (SdbInsertFp)mndUserActionInsert,
                     .updateFp = (SdbUpdateFp)mndUserActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndUserActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_USER, mndGetUserMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
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

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("user:%s, will be created while deploy sdb", userObj.user);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

#if 0
  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, "_" TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }
#endif

  return 0;
}

static SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, TSDB_USER_VER_NUMBER, sizeof(SUserObj) + TSDB_USER_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_USER_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_USER_VER_NUMBER) {
    mError("failed to decode user since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow  *pRow = sdbAllocRow(sizeof(SUserObj));
  SUserObj *pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->user, TSDB_USER_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->pass, TSDB_PASSWORD_LEN)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pUser->acct, TSDB_USER_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pUser->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pUser->updateTime)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pUser->superUser)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_USER_RESERVE_SIZE)

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

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOldUser, SUserObj *pNewUser) {
  mTrace("user:%s, perform update action", pOldUser->user);
  memcpy(pOldUser->pass, pNewUser->pass, TSDB_PASSWORD_LEN);
  pOldUser->updateTime = pNewUser->updateTime;
  return 0;
}

SUserObj *mndAcquireUser(SMnode *pMnode, char *userName) {
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
  }
  return pUser;
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
  userObj.superUser = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", user, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create user:%s", pTrans->id, user);

  SSdbRaw *pRedoRaw = mndUserActionEncode(&userObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateUserMsg(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  SCreateUserMsg *pCreate = pMsg->rpcMsg.pCont;

  mDebug("user:%s, start to create", pCreate->user);

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

static int32_t mndUpdateUser(SMnode *pMnode, SUserObj *pOldUser, SUserObj *pNewUser, SMnodeMsg *pMsg) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("user:%s, failed to update since %s", pOldUser->user, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to update user:%s", pTrans->id, pOldUser->user);

  SSdbRaw *pRedoRaw = mndUserActionEncode(pNewUser);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessAlterUserMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SAlterUserMsg *pAlter = pMsg->rpcMsg.pCont;

  mDebug("user:%s, start to alter", pAlter->user);

  if (pAlter->user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    mError("user:%s, failed to alter since %s", pAlter->user, terrstr());
    return -1;
  }

  if (pAlter->pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    mError("user:%s, failed to alter since %s", pAlter->user, terrstr());
    return -1;
  }

  SUserObj *pUser = sdbAcquire(pMnode->pSdb, SDB_USER, pAlter->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    mError("user:%s, failed to alter since %s", pAlter->user, terrstr());
    return -1;
  }

  SUserObj *pOperUser = sdbAcquire(pMnode->pSdb, SDB_USER, pMsg->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    mError("user:%s, failed to alter since %s", pAlter->user, terrstr());
    return -1;
  }

  SUserObj newUser = {0};
  memcpy(&newUser, pUser, sizeof(SUserObj));
  memset(pUser->pass, 0, sizeof(pUser->pass));
  taosEncryptPass((uint8_t *)pAlter->pass, strlen(pAlter->pass), pUser->pass);
  newUser.updateTime = taosGetTimestampMs();

  int32_t code = mndUpdateUser(pMnode, pUser, &newUser, pMsg);
  sdbRelease(pMnode->pSdb, pOperUser);

  if (code != 0) {
    mError("user:%s, failed to alter since %s", pAlter->user, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndDropUser(SMnode *pMnode, SMnodeMsg *pMsg, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pRedoRaw = mndUserActionEncode(pUser);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropUserMsg(SMnodeMsg *pMsg) {
  SMnode       *pMnode = pMsg->pMnode;
  SDropUserMsg *pDrop = pMsg->rpcMsg.pCont;

  mDebug("user:%s, start to drop", pDrop->user);

  if (pDrop->user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    mError("user:%s, failed to drop since %s", pDrop->user, terrstr());
    return -1;
  }

  SUserObj *pUser = sdbAcquire(pMnode->pSdb, SDB_USER, pDrop->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    mError("user:%s, failed to drop since %s", pDrop->user, terrstr());
    return -1;
  }

  SUserObj *pOperUser = sdbAcquire(pMnode->pSdb, SDB_USER, pMsg->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    mError("user:%s, failed to drop since %s", pDrop->user, terrstr());
    return -1;
  }

  int32_t code = mndDropUser(pMnode, pMsg, pUser);
  sdbRelease(pMnode->pSdb, pOperUser);

  if (code != 0) {
    mError("user:%s, failed to drop since %s", pDrop->user, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndGetUserMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "privilege");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "account");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_USER);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveUsers(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode   *pMnode = pMsg->pMnode;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->user, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pUser->superUser) {
      const char *src = "super";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else {
      const char *src = "normal";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pUser->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->acct, pShow->bytes[cols]);
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}