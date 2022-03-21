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
#include "mndAuth.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "tbase64.h"

#define TSDB_USER_VER_NUMBER 1
#define TSDB_USER_RESERVE_SIZE 64

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRaw *mndUserActionEncode(SUserObj *pUser);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SNodeMsg *pReq);
static int32_t  mndProcessCreateUserReq(SNodeMsg *pReq);
static int32_t  mndProcessAlterUserReq(SNodeMsg *pReq);
static int32_t  mndProcessDropUserReq(SNodeMsg *pReq);
static int32_t  mndProcessGetUserAuthReq(SNodeMsg *pReq);
static int32_t  mndGetUserMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveUsers(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
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

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_AUTH, mndProcessGetUserAuthReq);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_USER, mndGetUserMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
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

  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("user:%s, will be created while deploy sdb, raw:%p", userObj.user, pRaw);
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
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
  int32_t size = sizeof(SUserObj) + TSDB_USER_RESERVE_SIZE + (numOfReadDbs + numOfWriteDbs) * TSDB_DB_FNAME_LEN;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, TSDB_USER_VER_NUMBER, size);
  if (pRaw == NULL) goto USER_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, USER_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, USER_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, USER_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, USER_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, USER_ENCODE_OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, USER_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadDbs, USER_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteDbs, USER_ENCODE_OVER)

  char *db = taosHashIterate(pUser->readDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, USER_ENCODE_OVER);
    db = taosHashIterate(pUser->readDbs, db);
  }

  db = taosHashIterate(pUser->writeDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, USER_ENCODE_OVER);
    db = taosHashIterate(pUser->writeDbs, db);
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_USER_RESERVE_SIZE, USER_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, USER_ENCODE_OVER)

  terrno = 0;

USER_ENCODE_OVER:
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

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto USER_DECODE_OVER;

  if (sver != TSDB_USER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto USER_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) goto USER_DECODE_OVER;

  SUserObj *pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) goto USER_DECODE_OVER;

  pUser->readDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, true);
  pUser->writeDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, true);
  if (pUser->readDbs == NULL || pUser->writeDbs == NULL) goto USER_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, USER_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, USER_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, USER_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, USER_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, USER_DECODE_OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, USER_DECODE_OVER)

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  SDB_GET_INT32(pRaw, dataPos, &numOfReadDbs, USER_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &numOfWriteDbs, USER_DECODE_OVER)

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, USER_DECODE_OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->readDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, USER_DECODE_OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->writeDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_USER_RESERVE_SIZE, USER_DECODE_OVER)

  terrno = 0;

USER_DECODE_OVER:
  if (terrno != 0) {
    mError("user:%s, failed to decode from raw:%p since %s", pUser->user, pRaw, terrstr());
    taosHashCleanup(pUser->readDbs);
    taosHashCleanup(pUser->writeDbs);
    tfree(pRow);
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

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action, row:%p", pUser->user, pUser);
  taosHashCleanup(pUser->readDbs);
  taosHashCleanup(pUser->writeDbs);
  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew) {
  mTrace("user:%s, perform update action, old row:%p new row:%p", pOld->user, pOld, pNew);
  memcpy(pOld->pass, pNew->pass, TSDB_PASSWORD_LEN);
  pOld->updateTime = pNew->updateTime;

  void *tmp1 = pOld->readDbs;
  pOld->readDbs = pNew->readDbs;
  pNew->readDbs = tmp1;

  void *tmp2 = pOld->writeDbs;
  pOld->writeDbs = pNew->writeDbs;
  pNew->writeDbs = tmp2;

  return 0;
}

SUserObj *mndAcquireUser(SMnode *pMnode, char *userName) {
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

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SNodeMsg *pReq) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = pCreate->superUser;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK,TRN_TYPE_CREATE_USER, &pReq->rpcMsg);
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

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

static int32_t mndProcessCreateUserReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};

  if (tDeserializeSCreateUserReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_USER_OVER;
  }

  mDebug("user:%s, start to create", createReq.user);

  if (createReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto CREATE_USER_OVER;
  }

  if (createReq.pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto CREATE_USER_OVER;
  }

  pUser = mndAcquireUser(pMnode, createReq.user);
  if (pUser != NULL) {
    terrno = TSDB_CODE_MND_USER_ALREADY_EXIST;
    goto CREATE_USER_OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto CREATE_USER_OVER;
  }

  if (mndCheckCreateUserAuth(pOperUser) != 0) {
    goto CREATE_USER_OVER;
  }

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_USER_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create since %s", createReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);

  return code;
}

static int32_t mndUpdateUser(SMnode *pMnode, SUserObj *pOld, SUserObj *pNew, SNodeMsg *pReq) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_ALTER_USER,&pReq->rpcMsg);
  if (pTrans == NULL) {
    mError("user:%s, failed to update since %s", pOld->user, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to update user:%s", pTrans->id, pOld->user);

  SSdbRaw *pRedoRaw = mndUserActionEncode(pNew);
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

static SHashObj *mndDupDbHash(SHashObj *pOld) {
  SHashObj *pNew = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, true);
  if (pNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if (taosHashPut(pNew, db, len, db, TSDB_DB_FNAME_LEN) != 0) {
      taosHashCancelIterate(pOld, db);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosHashCleanup(pNew);
      return NULL;
    }
    db = taosHashIterate(pOld, db);
  }

  return pNew;
}

static int32_t mndProcessAlterUserReq(SNodeMsg *pReq) {
  SMnode       *pMnode = pReq->pNode;
  int32_t       code = -1;
  SUserObj     *pUser = NULL;
  SUserObj     *pOperUser = NULL;
  SUserObj      newUser = {0};
  SAlterUserReq alterReq = {0};

  if (tDeserializeSAlterUserReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto ALTER_USER_OVER;
  }

  mDebug("user:%s, start to alter", alterReq.user);

  if (alterReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto ALTER_USER_OVER;
  }

  if (alterReq.pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto ALTER_USER_OVER;
  }

  pUser = mndAcquireUser(pMnode, alterReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto ALTER_USER_OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto ALTER_USER_OVER;
  }

  memcpy(&newUser, pUser, sizeof(SUserObj));
  newUser.readDbs = mndDupDbHash(pUser->readDbs);
  newUser.writeDbs = mndDupDbHash(pUser->writeDbs);
  if (newUser.readDbs == NULL || newUser.writeDbs == NULL) {
    goto ALTER_USER_OVER;
  }

  int32_t len = strlen(alterReq.dbname) + 1;
  SDbObj *pDb = mndAcquireDb(pMnode, alterReq.dbname);
  mndReleaseDb(pMnode, pDb);

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)alterReq.pass, strlen(alterReq.pass), pass);
    memcpy(pUser->pass, pass, TSDB_PASSWORD_LEN);
  } else if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER) {
    newUser.superUser = alterReq.superUser;
  } else if (alterReq.alterType == TSDB_ALTER_USER_ADD_READ_DB) {
    if (pDb == NULL) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto ALTER_USER_OVER;
    }
    if (taosHashPut(newUser.readDbs, alterReq.dbname, len, alterReq.dbname, TSDB_DB_FNAME_LEN) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto ALTER_USER_OVER;
    }
  } else if (alterReq.alterType == TSDB_ALTER_USER_REMOVE_READ_DB) {
    if (taosHashRemove(newUser.readDbs, alterReq.dbname, len) != 0) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto ALTER_USER_OVER;
    }
  } else if (alterReq.alterType == TSDB_ALTER_USER_CLEAR_READ_DB) {
    taosHashClear(newUser.readDbs);
  } else if (alterReq.alterType == TSDB_ALTER_USER_ADD_WRITE_DB) {
    if (pDb == NULL) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto ALTER_USER_OVER;
    }
    if (taosHashPut(newUser.writeDbs, alterReq.dbname, len, alterReq.dbname, TSDB_DB_FNAME_LEN) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto ALTER_USER_OVER;
    }
  } else if (alterReq.alterType == TSDB_ALTER_USER_REMOVE_WRITE_DB) {
    if (taosHashRemove(newUser.writeDbs, alterReq.dbname, len) != 0) {
      terrno = TSDB_CODE_MND_DB_NOT_EXIST;
      goto ALTER_USER_OVER;
    }
  } else if (alterReq.alterType == TSDB_ALTER_USER_CLEAR_WRITE_DB) {
    taosHashClear(newUser.writeDbs);
  } else {
    terrno = TSDB_CODE_MND_INVALID_ALTER_OPER;
    goto ALTER_USER_OVER;
  }

  newUser.updateTime = taosGetTimestampMs();

  if (mndCheckAlterUserAuth(pOperUser, pUser, pDb, &alterReq) != 0) {
    goto ALTER_USER_OVER;
  }

  code = mndUpdateUser(pMnode, pUser, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

ALTER_USER_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter since %s", alterReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);
  taosHashCleanup(newUser.writeDbs);
  taosHashCleanup(newUser.readDbs);

  return code;
}

static int32_t mndDropUser(SMnode *pMnode, SNodeMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK,TRN_TYPE_DROP_USER, &pReq->rpcMsg);
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

static int32_t mndProcessDropUserReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SUserObj    *pOperUser = NULL;
  SDropUserReq dropReq = {0};

  if (tDeserializeSDropUserReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_USER_OVER;
  }

  mDebug("user:%s, start to drop", dropReq.user);

  if (dropReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto DROP_USER_OVER;
  }

  pUser = mndAcquireUser(pMnode, dropReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto DROP_USER_OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto DROP_USER_OVER;
  }

  if (mndCheckDropUserAuth(pOperUser) != 0) {
    goto DROP_USER_OVER;
  }

  code = mndDropUser(pMnode, pReq, pUser);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_USER_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop since %s", dropReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessGetUserAuthReq(SNodeMsg *pReq) {
  SMnode         *pMnode = pReq->pNode;
  int32_t         code = -1;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  if (tDeserializeSGetUserAuthReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &authReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto GET_AUTH_OVER;
  }

  mTrace("user:%s, start to get auth", authReq.user);

  pUser = mndAcquireUser(pMnode, authReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto GET_AUTH_OVER;
  }

  memcpy(authRsp.user, pUser->user, TSDB_USER_LEN);
  authRsp.superAuth = pUser->superUser;
  authRsp.readDbs = mndDupDbHash(pUser->readDbs);
  authRsp.writeDbs = mndDupDbHash(pUser->writeDbs);

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;

    if (strcmp(pDb->createUser, pUser->user) == 0) {
      int32_t len = strlen(pDb->name) + 1;
      taosHashPut(authRsp.readDbs, pDb->name, len, pDb->name, len);
      taosHashPut(authRsp.writeDbs, pDb->name, len, pDb->name, len);
    }

    sdbRelease(pSdb, pDb);
  }

  int32_t contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto GET_AUTH_OVER;
  }

  tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);

  pReq->pRsp = pRsp;
  pReq->rspLen = contLen;
  code = 0;

GET_AUTH_OVER:
  mndReleaseUser(pMnode, pUser);
  taosHashCleanup(authRsp.readDbs);
  taosHashCleanup(authRsp.writeDbs);

  return code;
}

static int32_t mndGetUserMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "privilege");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "account");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_USER);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveUsers(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode   *pMnode = pReq->pNode;
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
