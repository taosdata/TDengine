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
#include "mnodeSync.h"
#include "os.h"
#include "tglobal.h"
#include "tkey.h"

#define USER_VER 1

static SSdbRaw *mnodeUserActionEncode(SUserObj *pUser) {
  SSdbRaw *pRaw = calloc(1, sizeof(SUserObj) + sizeof(SSdbRaw));
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t dataLen = 0;
  char   *pData = pRaw->data;
  SDB_SET_BINARY_VAL(pData, dataLen, pUser->user, TSDB_USER_LEN)
  SDB_SET_BINARY_VAL(pData, dataLen, pUser->pass, TSDB_KEY_LEN)
  SDB_SET_BINARY_VAL(pData, dataLen, pUser->acct, TSDB_KEY_LEN)
  SDB_SET_INT64_VAL(pData, dataLen, pUser->createdTime)
  SDB_SET_INT64_VAL(pData, dataLen, pUser->updateTime)
  SDB_SET_INT32_VAL(pData, dataLen, pUser->rootAuth)

  pRaw->dataLen = dataLen;
  pRaw->type = SDB_USER;
  pRaw->sver = USER_VER;
  return pRaw;
}

static SUserObj *mnodeUserActionDecode(SSdbRaw *pRaw) {
  if (pRaw->sver != USER_VER) {
    terrno = TSDB_CODE_SDB_INVAID_RAW_DATA_VER;
    return NULL;
  }

  SUserObj *pUser = calloc(1, sizeof(SUserObj));
  if (pUser == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t code = 0;
  int32_t dataLen = pRaw->dataLen;
  char   *pData = pRaw->data;
  SDB_GET_BINARY_VAL(pData, dataLen, pUser->user, TSDB_USER_LEN, code)
  SDB_GET_BINARY_VAL(pData, dataLen, pUser->pass, TSDB_KEY_LEN, code)
  SDB_GET_BINARY_VAL(pData, dataLen, pUser->acct, TSDB_USER_LEN, code)
  SDB_GET_INT64_VAL(pData, dataLen, pUser->createdTime, code)
  SDB_GET_INT64_VAL(pData, dataLen, pUser->updateTime, code)
  SDB_GET_INT32_VAL(pData, dataLen, pUser->rootAuth, code)

  if (code != 0) {
    tfree(pUser);
    terrno = code;
    return NULL;
  }

  return pUser;
}

static int32_t mnodeUserActionInsert(SUserObj *pUser) {
  pUser->prohibitDbHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->prohibitDbHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pUser->pAcct = sdbAcquire(SDB_ACCT, pUser->acct);
  if (pUser->pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    return -1;
  }

  return 0;
}

static int32_t mnodeUserActionDelete(SUserObj *pUser) {
  if (pUser->prohibitDbHash) {
    taosHashCleanup(pUser->prohibitDbHash);
    pUser->prohibitDbHash = NULL;
  }

  if (pUser->acct != NULL) {
    sdbRelease(pUser->pAcct);
    pUser->pAcct = NULL;
  }

  return 0;
}

static int32_t mnodeUserActionUpdate(SUserObj *pSrcUser, SUserObj *pDstUser) {
  memcpy(pDstUser, pSrcUser, (int32_t)((char *)&pDstUser->prohibitDbHash - (char *)&pDstUser));
  return 0;
}

static int32_t mnodeCreateDefaultUser(char *acct, char *user, char *pass) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.rootAuth = 1;
  }

  SSdbRaw *pRaw = mnodeUserActionEncode(&userObj);
  if (pRaw == NULL) {
    return -1;
  }

  return sdbWrite(pRaw);
}

static int32_t mnodeCreateDefaultUsers() {
  if (mnodeCreateDefaultUser(TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  if (mnodeCreateDefaultUser(TSDB_DEFAULT_USER, "monitor", tsInternalPass) != 0) {
    return -1;
  }

  if (mnodeCreateDefaultUser(TSDB_DEFAULT_USER, "_" TSDB_DEFAULT_USER, tsInternalPass) != 0) {
    return -1;
  }

  return 0;
}

static int32_t mnodeCreateUser(char *acct, char *user, char *pass, SMnodeMsg *pMsg) {
  SUserObj userObj = {0};
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  taosEncryptPass((uint8_t *)pass, strlen(pass), userObj.pass);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.rootAuth = 0;

  STrans *pTrans = trnCreate(TRN_POLICY_ROLLBACK);
  if (pTrans == NULL) return -1;

  SSdbRaw *pRedoRaw = mnodeUserActionEncode(&userObj);
  if (pRedoRaw == NULL || trnAppendRedoLog(pTrans, pRedoRaw) != 0) {
    trnDrop(pTrans);
    return -1;
  }
  pRedoRaw->status = SDB_STATUS_CREATING;
  pRedoRaw->action = SDB_ACTION_INSERT;

  SSdbRaw *pUndoRaw = mnodeUserActionEncode(&userObj);
  if (pUndoRaw == NULL || trnAppendUndoLog(pTrans, pUndoRaw) != 0) {
    trnDrop(pTrans);
    return -1;
  }
  pUndoRaw->status = SDB_STATUS_DROPPING;
  pUndoRaw->action = SDB_ACTION_DELETE;

  SSdbRaw *pCommitRaw = mnodeUserActionEncode(&userObj);
  if (pCommitRaw == NULL || trnAppendCommitLog(pTrans, pCommitRaw) != 0) {
    trnDrop(pTrans);
    return -1;
  }
  pCommitRaw->status = SDB_STATUS_READY;
  pCommitRaw->action = SDB_ACTION_UPDATE;

  trnSetRpcHandle(pTrans, pMsg->rpcMsg.handle);

  if (trnPrepare(pTrans, mnodeSyncPropose) != 0) {
    trnDrop(pTrans);
    return -1;
  }

  trnDrop(pTrans);
  return 0;
}

static int32_t mnodeProcessCreateUserMsg(SMnodeMsg *pMsg) {
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

  SUserObj *pUser = sdbAcquire(SDB_USER, pCreate->user);
  if (pUser != NULL) {
    sdbRelease(pUser);
    terrno = TSDB_CODE_MND_USER_ALREADY_EXIST;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  SUserObj *pOperUser = sdbAcquire(SDB_USER, pMsg->conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  int32_t code = mnodeCreateUser(pOperUser->acct, pCreate->user, pCreate->pass, pMsg);
  sdbRelease(pOperUser);

  if (code != 0) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

int32_t mnodeInitUser() {
  SSdbHandle handle = {.sdbType = SDB_USER,
                       .keyType = SDB_KEY_BINARY,
                       .deployFp = (SdbDeployFp)mnodeCreateDefaultUsers,
                       .encodeFp = (SdbEncodeFp)mnodeUserActionEncode,
                       .decodeFp = (SdbDecodeFp)mnodeUserActionDecode,
                       .insertFp = (SdbInsertFp)mnodeUserActionInsert,
                       .updateFp = (SdbUpdateFp)mnodeUserActionUpdate,
                       .deleteFp = (SdbDeleteFp)mnodeUserActionDelete};
  sdbSetHandle(handle);

  return 0;
}

void mnodeCleanupUser() {}