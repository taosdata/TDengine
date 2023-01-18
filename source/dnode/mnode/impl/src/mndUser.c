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
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

#define USER_VER_NUMBER   2
#define USER_RESERVE_SIZE 64

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateUserReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterUserReq(SRpcMsg *pReq);
static int32_t  mndProcessDropUserReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserAuthReq(SRpcMsg *pReq);
static int32_t  mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);

int32_t mndInitUser(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
      .encodeFp = (SdbEncodeFp)mndUserActionEncode,
      .decodeFp = (SdbDecodeFp)mndUserActionDecode,
      .insertFp = (SdbInsertFp)mndUserActionInsert,
      .updateFp = (SdbUpdateFp)mndUserActionUpdate,
      .deleteFp = (SdbDeleteFp)mndUserActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_AUTH, mndProcessGetUserAuthReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupUser(SMnode *pMnode) {}

static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.pass);
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.sysInfo = 1;
  userObj.enable = 1;

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) return -1;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mInfo("user:%s, will be created when deploying, raw:%p", userObj.user, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-user");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("user:%s, failed to create since %s", userObj.user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, userObj.user);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  return 0;
}

SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
  int32_t numOfTopics = taosHashGetSize(pUser->topics);
  int32_t size = sizeof(SUserObj) + USER_RESERVE_SIZE + (numOfReadDbs + numOfWriteDbs) * TSDB_DB_FNAME_LEN +
                 numOfTopics * TSDB_TOPIC_FNAME_LEN;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, USER_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->sysInfo, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->enable, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->reserve, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->authVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfTopics, _OVER)

  char *db = taosHashIterate(pUser->readDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->readDbs, db);
  }

  db = taosHashIterate(pUser->writeDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->writeDbs, db);
  }

  char *topic = taosHashIterate(pUser->topics, NULL);
  while (topic != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER);
    topic = taosHashIterate(pUser->topics, topic);
  }

  SDB_SET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("user:%s, failed to encode to raw:%p since %s", pUser->user, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("user:%s, encode to raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow  *pRow = NULL;
  SUserObj *pUser = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != 1 && sver != 2) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) goto _OVER;

  pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->sysInfo, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->enable, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->reserve, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pUser->authVersion, _OVER)

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  int32_t numOfTopics = 0;
  SDB_GET_INT32(pRaw, dataPos, &numOfReadDbs, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &numOfWriteDbs, _OVER)
  if (sver >= 2) {
    SDB_GET_INT32(pRaw, dataPos, &numOfTopics, _OVER)
  }

  pUser->readDbs = taosHashInit(numOfReadDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->writeDbs =
      taosHashInit(numOfWriteDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->topics = taosHashInit(numOfTopics, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->readDbs == NULL || pUser->writeDbs == NULL || pUser->topics == NULL) goto _OVER;

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->readDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->writeDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  if (sver >= 2) {
    for (int32_t i = 0; i < numOfTopics; ++i) {
      char topic[TSDB_TOPIC_FNAME_LEN] = {0};
      SDB_GET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER)
      int32_t len = strlen(topic) + 1;
      taosHashPut(pUser->topics, topic, len, topic, TSDB_TOPIC_FNAME_LEN);
    }
  }

  SDB_GET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pUser->lock);

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("user:%s, failed to decode from raw:%p since %s", pUser == NULL ? "null" : pUser->user, pRaw, terrstr());
    if (pUser != NULL) {
      taosHashCleanup(pUser->readDbs);
      taosHashCleanup(pUser->writeDbs);
      taosHashCleanup(pUser->topics);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("user:%s, decode from raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform insert action, row:%p", pUser->user, pUser);

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

static int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew) {
  memcpy(pNew, pUser, sizeof(SUserObj));
  pNew->authVersion++;
  pNew->updateTime = taosGetTimestampMs();

  taosRLockLatch(&pUser->lock);
  pNew->readDbs = mndDupDbHash(pUser->readDbs);
  pNew->writeDbs = mndDupDbHash(pUser->writeDbs);
  pNew->topics = mndDupTopicHash(pUser->topics);
  taosRUnLockLatch(&pUser->lock);

  if (pNew->readDbs == NULL || pNew->writeDbs == NULL || pNew->topics == NULL) {
    return -1;
  }
  return 0;
}

static void mndUserFreeObj(SUserObj *pUser) {
  taosHashCleanup(pUser->readDbs);
  taosHashCleanup(pUser->writeDbs);
  taosHashCleanup(pUser->topics);
  pUser->readDbs = NULL;
  pUser->writeDbs = NULL;
  pUser->topics = NULL;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action, row:%p", pUser->user, pUser);
  mndUserFreeObj(pUser);
  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew) {
  mTrace("user:%s, perform update action, old row:%p new row:%p", pOld->user, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->authVersion = pNew->authVersion;
  pOld->sysInfo = pNew->sysInfo;
  pOld->enable = pNew->enable;
  memcpy(pOld->pass, pNew->pass, TSDB_PASSWORD_LEN);
  TSWAP(pOld->readDbs, pNew->readDbs);
  TSWAP(pOld->writeDbs, pNew->writeDbs);
  TSWAP(pOld->topics, pNew->topics);
  taosWUnLockLatch(&pOld->lock);

  return 0;
}

SUserObj *mndAcquireUser(SMnode *pMnode, const char *userName) {
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
  }
  return pUser;
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateUserReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};

  if (tDeserializeSCreateUserReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to create", createReq.user);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_USER) != 0) {
    goto _OVER;
  }

  if (createReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  if (createReq.pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, createReq.user);
  if (pUser != NULL) {
    terrno = TSDB_CODE_MND_USER_ALREADY_EXIST;
    goto _OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if ((terrno = grantCheck(TSDB_GRANT_USER)) != 0) {
    code = terrno;
    goto _OVER;
  }

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create since %s", createReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);

  return code;
}

static int32_t mndAlterUser(SMnode *pMnode, SUserObj *pOld, SUserObj *pNew, SRpcMsg *pReq) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "alter-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to alter since %s", pOld->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to alter user:%s", pTrans->id, pOld->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pNew);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

SHashObj *mndDupObjHash(SHashObj *pOld, int32_t dataLen) {
  SHashObj *pNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if (taosHashPut(pNew, db, len, db, dataLen) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(pNew);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    db = taosHashIterate(pOld, db);
  }

  return pNew;
}

SHashObj *mndDupDbHash(SHashObj *pOld) { return mndDupObjHash(pOld, TSDB_DB_FNAME_LEN); }

SHashObj *mndDupTopicHash(SHashObj *pOld) { return mndDupObjHash(pOld, TSDB_TOPIC_FNAME_LEN); }

static int32_t mndProcessAlterUserReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  void         *pIter = NULL;
  int32_t       code = -1;
  SUserObj     *pUser = NULL;
  SUserObj     *pOperUser = NULL;
  SUserObj      newUser = {0};
  SAlterUserReq alterReq = {0};

  if (tDeserializeSAlterUserReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to alter", alterReq.user);

  if (alterReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  if (TSDB_ALTER_USER_PASSWD == alterReq.alterType && alterReq.pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, alterReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckAlterUserPrivilege(pOperUser, pUser, &alterReq) != 0) {
    goto _OVER;
  }

  if (mndUserDupObj(pUser, &newUser) != 0) goto _OVER;

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)alterReq.pass, strlen(alterReq.pass), pass);
    memcpy(newUser.pass, pass, TSDB_PASSWORD_LEN);
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER) {
    newUser.superUser = alterReq.superUser;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ENABLE) {
    newUser.enable = alterReq.enable;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SYSINFO) {
    newUser.sysInfo = alterReq.sysInfo;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_READ_DB || alterReq.alterType == TSDB_ALTER_USER_ADD_ALL_DB) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      int32_t len = strlen(alterReq.objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, alterReq.objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
      if (taosHashPut(newUser.readDbs, alterReq.objname, len, alterReq.objname, TSDB_DB_FNAME_LEN) != 0) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        taosHashPut(newUser.readDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN);
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_WRITE_DB || alterReq.alterType == TSDB_ALTER_USER_ADD_ALL_DB) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      int32_t len = strlen(alterReq.objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, alterReq.objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
      if (taosHashPut(newUser.writeDbs, alterReq.objname, len, alterReq.objname, TSDB_DB_FNAME_LEN) != 0) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        taosHashPut(newUser.writeDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN);
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_REMOVE_READ_DB || alterReq.alterType == TSDB_ALTER_USER_REMOVE_ALL_DB) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      int32_t len = strlen(alterReq.objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, alterReq.objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
      taosHashRemove(newUser.readDbs, alterReq.objname, len);
    } else {
      taosHashClear(newUser.readDbs);
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_REMOVE_WRITE_DB || alterReq.alterType == TSDB_ALTER_USER_REMOVE_ALL_DB) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      int32_t len = strlen(alterReq.objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, alterReq.objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        goto _OVER;
      }
      taosHashRemove(newUser.writeDbs, alterReq.objname, len);
    } else {
      taosHashClear(newUser.writeDbs);
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_SUBSCRIBE_TOPIC) {
    int32_t      len = strlen(alterReq.objname) + 1;
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, alterReq.objname);
    if (pTopic == NULL) {
      mndReleaseTopic(pMnode, pTopic);
      goto _OVER;
    }
    taosHashPut(newUser.topics, pTopic->name, len, pTopic->name, TSDB_TOPIC_FNAME_LEN);
  }

  if (alterReq.alterType == TSDB_ALTER_USER_REMOVE_SUBSCRIBE_TOPIC) {
    int32_t      len = strlen(alterReq.objname) + 1;
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, alterReq.objname);
    if (pTopic == NULL) {
      mndReleaseTopic(pMnode, pTopic);
      goto _OVER;
    }
    taosHashRemove(newUser.topics, alterReq.objname, len);
  }

  code = mndAlterUser(pMnode, pUser, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter since %s", alterReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);

  return code;
}

static int32_t mndDropUser(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropUserReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SDropUserReq dropReq = {0};

  if (tDeserializeSDropUserReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to drop", dropReq.user);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_USER) != 0) {
    goto _OVER;
  }

  if (dropReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, dropReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  code = mndDropUser(pMnode, pReq, pUser);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop since %s", dropReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pUser);
  return code;
}

static int32_t mndProcessGetUserAuthReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  if (tDeserializeSGetUserAuthReq(pReq->pCont, pReq->contLen, &authReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mTrace("user:%s, start to get auth", authReq.user);

  pUser = mndAcquireUser(pMnode, authReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  code = mndSetUserAuthRsp(pMnode, pUser, &authRsp);
  if (code) {
    goto _OVER;
  }

  int32_t contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  code = 0;

_OVER:

  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserAuthRsp(&authRsp);

  return code;
}

static int32_t mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    colDataAppend(pColInfo, numOfRows, (const char *)name, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppend(pColInfo, numOfRows, (const char *)&pUser->superUser, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppend(pColInfo, numOfRows, (const char *)&pUser->enable, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppend(pColInfo, numOfRows, (const char *)&pUser->sysInfo, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppend(pColInfo, numOfRows, (const char *)&pUser->createdTime, false);

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
    int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
    int32_t numOfTopics = taosHashGetSize(pUser->topics);
    if (numOfRows + numOfReadDbs + numOfWriteDbs + numOfTopics >= rows) break;

    if (pUser->superUser) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)privilege, false);

      char objName[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(objName, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)objName, false);

      numOfRows++;
    }

    char *db = taosHashIterate(pUser->readDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "read", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)privilege, false);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)objName, false);

      numOfRows++;
      db = taosHashIterate(pUser->readDbs, db);
    }

    db = taosHashIterate(pUser->writeDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "write", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)privilege, false);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)objName, false);

      numOfRows++;
      db = taosHashIterate(pUser->writeDbs, db);
    }

    char *topic = taosHashIterate(pUser->topics, NULL);
    while (topic != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "subscribe", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)privilege, false);

      char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
      tstrncpy(varDataVal(topicName), mndGetDbStr(topic), TSDB_TOPIC_NAME_LEN - 2);
      varDataSetLen(topicName, strlen(varDataVal(topicName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)topicName, false);

      numOfRows++;
      topic = taosHashIterate(pUser->topics, topic);
    }

    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                int32_t *pRspLen) {
  SUserAuthBatchRsp batchRsp = {0};
  batchRsp.pArray = taosArrayInit(numOfUses, sizeof(SGetUserAuthRsp));
  if (batchRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t code = 0;
  for (int32_t i = 0; i < numOfUses; ++i) {
    SUserObj *pUser = mndAcquireUser(pMnode, pUsers[i].user);
    if (pUser == NULL) {
      mError("user:%s, failed to auth user since %s", pUsers[i].user, terrstr());
      continue;
    }

    pUsers[i].version = ntohl(pUsers[i].version);
    if (pUser->authVersion <= pUsers[i].version) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }

    SGetUserAuthRsp rsp = {0};
    code = mndSetUserAuthRsp(pMnode, pUser, &rsp);
    if (code) {
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      goto _OVER;
    }

    taosArrayPush(batchRsp.pArray, &rsp);
    mndReleaseUser(pMnode, pUser);
  }

  if (taosArrayGetSize(batchRsp.pArray) <= 0) {
    *ppRsp = NULL;
    *pRspLen = 0;

    tFreeSUserAuthBatchRsp(&batchRsp);
    return 0;
  }

  int32_t rspLen = tSerializeSUserAuthBatchRsp(NULL, 0, &batchRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeSUserAuthBatchRsp(&batchRsp);
    return -1;
  }
  tSerializeSUserAuthBatchRsp(pRsp, rspLen, &batchRsp);

  *ppRsp = pRsp;
  *pRspLen = rspLen;

  tFreeSUserAuthBatchRsp(&batchRsp);
  return 0;

_OVER:

  *ppRsp = NULL;
  *pRspLen = 0;

  tFreeSUserAuthBatchRsp(&batchRsp);
  return code;
}

int32_t mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, char *db) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(db) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) break;

    bool inRead = (taosHashGet(newUser.readDbs, db, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeDbs, db, len) != NULL);
    if (inRead || inWrite) {
      (void)taosHashRemove(newUser.readDbs, db, len);
      (void)taosHashRemove(newUser.writeDbs, db, len);

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) break;
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}

int32_t mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(topic) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) break;

    bool inTopic = (taosHashGet(newUser.topics, topic, len) != NULL);
    if (inTopic) {
      (void)taosHashRemove(newUser.topics, topic, len);
      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) break;
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}
