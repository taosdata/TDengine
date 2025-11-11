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
// clang-format off
#ifndef TD_ASTRA
#include <uv.h>
#endif
#include "crypt.h"
#include "mndRole.h"
#include "mndUser.h"
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

// clang-format on

#define MND_ROLE_VER_NUMBER                      1

static int32_t  mndCreateDefaultRoles(SMnode *pMnode);
static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw);
static int32_t  mndRoleActionInsert(SSdb *pSdb, SRoleObj *pRole);
static int32_t  mndRoleActionDelete(SSdb *pSdb, SRoleObj *pRole);
static int32_t  mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew);
static int32_t  mndCreateRole(SMnode *pMnode, char *acct, SCreateRoleReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessDropRoleReq(SRpcMsg *pReq);
static int32_t  mndProcessGetRoleAuthReq(SRpcMsg *pReq);
static int32_t  mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextRole(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);



int32_t mndInitRole(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_ROLE,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultRoles,
      .encodeFp = (SdbEncodeFp)mndRoleActionEncode,
      .decodeFp = (SdbDecodeFp)mndRoleActionDecode,
      .insertFp = (SdbInsertFp)mndRoleActionInsert,
      .updateFp = (SdbUpdateFp)mndRoleActionUpdate,
      .deleteFp = (SdbDeleteFp)mndRoleActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ROLE, mndProcessCreateRoleReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ROLE, mndProcessDropRoleReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ROLE, mndRetrieveRoles);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ROLE, mndCancelGetNextRole);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRole(SMnode *pMnode) {  }

static int32_t mndCreateDefaultRole(SMnode *pMnode, char *acct, char *user, char *pass) {
//   int32_t  code = 0;
//   int32_t  lino = 0;
//   SRoleObj userObj = {0};
//   taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.pass);
//   tstrncpy(userObj.user, user, TSDB_ROLE_LEN);
//   tstrncpy(userObj.acct, acct, TSDB_ROLE_LEN);
//   userObj.createdTime = taosGetTimestampMs();
//   userObj.updateTime = userObj.createdTime;
//   userObj.sysInfo = 1;
//   userObj.enable = 1;
//   userObj.ipWhiteListVer = taosGetTimestampMs();
//   TAOS_CHECK_RETURN(createDefaultIpWhiteList(&userObj.pIpWhiteListDual));
//   if (strcmp(user, TSDB_DEFAULT_ROLE) == 0) {
//     userObj.superRole = 1;
//     userObj.createdb = 1;
//   }

//   SSdbRaw *pRaw = mndRoleActionEncode(&userObj);
//   if (pRaw == NULL) goto _ERROR;
//   TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

//   mInfo("user:%s, will be created when deploying, raw:%p", userObj.user, pRaw);

//   STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-user");
//   if (pTrans == NULL) {
//     sdbFreeRaw(pRaw);
//     mError("user:%s, failed to create since %s", userObj.user, terrstr());
//     goto _ERROR;
//   }
//   mInfo("trans:%d, used to create user:%s", pTrans->id, userObj.user);

//   if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
//     mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
//     mndTransDrop(pTrans);
//     goto _ERROR;
//   }
//   TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

//   if (mndTransPrepare(pMnode, pTrans) != 0) {
//     mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
//     mndTransDrop(pTrans);
//     goto _ERROR;
//   }

//   mndTransDrop(pTrans);
//   taosMemoryFree(userObj.pIpWhiteListDual);
//   return 0;
// _ERROR:
//   taosMemoryFree(userObj.pIpWhiteListDual);
//   TAOS_RETURN(terrno ? terrno : TSDB_CODE_APP_ERROR);
return 0;
}

static int32_t mndCreateDefaultRoles(SMnode *pMnode) {
//   return mndCreateDefaultRole(pMnode, TSDB_DEFAULT_ROLE, TSDB_DEFAULT_ROLE, TSDB_DEFAULT_PASS);
return 0;
}

static int32_t tSerializeSRoleObj(void *buf, int32_t bufLen, const SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, PRIV_GROUP_CNT));
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pObj->privSet.set[i]));
  }
  size_t  klen = 0;
  int32_t nParents = taosHashGetSize(pObj->parents);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nParents));
  if (nParents > 0) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(pObj->parents, pIter)) {
      char *roleName = taosHashGetKey(pIter, &klen);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, roleName));
    }
  }
  int32_t nChildren = taosHashGetSize(pObj->children);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nChildren));
  if (nChildren > 0) {
    void *pIter = NULL;
    while (pIter = taosHashIterate(pObj->children, pIter)) {
      char *roleName = taosHashGetKey(pIter, &klen);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, roleName));
    }
  }

  tEndEncode(&encoder);
  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("role:%s, %s failed at line %d since %s", pObj->name, __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeSRoleObj(void *buf, int32_t bufLen, SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  uint8_t nPrivGroups = 0;
  TAOS_CHECK_EXIT(tDecodeU8(&decoder, &nPrivGroups));
  int32_t nRealGroups = nPrivGroups > PRIV_GROUP_CNT ? PRIV_GROUP_CNT : nPrivGroups;
  for (int32_t i = 0; i < nRealGroups; ++i) {
    TAOS_CHECK_EXIT(tDecodeU64v(&decoder, &pObj->privSet.set[i]));
  }
  char    roleName[TSDB_ROLE_LEN] = {0};
  int32_t nParents = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nParents));
  if (nParents > 0) {
    if (!(pObj->parents =
              taosHashInit(nParents, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nParents; ++i) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, roleName));
      TAOS_CHECK_EXIT(taosHashPut(pObj->parents, roleName, strlen(roleName), NULL, 0));
    }
  }

  int32_t nChildren = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nChildren));
  if (nChildren > 0) {
    if (!(pObj->children =
              taosHashInit(nChildren, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK))) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nChildren; ++i) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, roleName));
      TAOS_CHECK_EXIT(taosHashPut(pObj->children, roleName, strlen(roleName), NULL, 0));
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("role, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndRoleActionEncode(SRoleObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSRoleObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }
  int32_t size = sizeof(int32_t) + tlen;
  if (!(pRaw = sdbAllocRaw(SDB_ROLE, MND_ROLE_VER_NUMBER, size))) {
    TAOS_CHECK_EXIT(terrno);
  }
  if (!(buf = taosMemoryMalloc(tlen))) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((tlen = tSerializeSRoleObj(buf, tlen, pObj)) < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("role, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }
  mTrace("role, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0, lino = 0;
  SSdbRow  *pRow = NULL;
  SRoleObj *pObj = NULL;
  void     *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_ROLE_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("role, read invalid ver, data ver: %d, curr ver: %d", sver, MND_ROLE_VER_NUMBER);
    TAOS_CHECK_EXIT(code);
  }

  if (!(pRow = sdbAllocRow(sizeof(SRoleObj)))) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    TAOS_CHECK_EXIT(terrno);
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  if (!(buf = taosMemoryMalloc(tlen + 1))) {
    TAOS_CHECK_EXIT(terrno);
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  TAOS_CHECK_EXIT(tDeserializeSRoleObj(buf, tlen, pObj));
  taosInitRWLatch(&pObj->lock);
_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("role, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    mndRoleFreeObj(pObj);
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("role, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

int32_t mndRoleDupObj(SRoleObj *pRole, SRoleObj *pNew) { return 0; }

void mndRoleFreeObj(SRoleObj *pObj) {
  if (pObj) {
    if (pObj->parents) {
      taosHashCleanup(pObj->parents);
      pObj->parents = NULL;
    }
    if (pObj->children) {
      taosHashCleanup(pObj->children);
      pObj->children = NULL;
    }
  }
}

static int32_t mndRoleActionInsert(SSdb *pSdb, SRoleObj *pObj) {
  mTrace("role:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndRoleActionDelete(SSdb *pSdb, SRoleObj *pObj) {
  mTrace("role:%s, perform delete action, row:%p", pObj->name, pObj);
  mndRoleFreeObj(pObj);
  return 0;
}

static int32_t mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew) {
  mTrace("role:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->privSet = pNew->privSet;
  TSWAP(pOld->parents, pNew->parents);
  TSWAP(pOld->children, pNew->children);
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

int32_t mndAcquireRole(SMnode *pMnode, const char *userName, SRoleObj **ppRole) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppRole = sdbAcquire(pSdb, SDB_ROLE, userName);
  if (*ppRole == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_ROLE_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_ROLE_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseRole(SMnode *pMnode, SRoleObj *pRole) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pRole);
}

static int32_t mndCreateRole(SMnode *pMnode, char *acct, SCreateRoleReq *pCreate, SRpcMsg *pReq) {
  int32_t  code = 0, lino = 0;
  SRoleObj obj = {0};

  tstrncpy(obj.name, pCreate->name, TSDB_ROLE_LEN);
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime = obj.createdTime;
  obj.uid = mndGenerateUid(obj.name, strlen(obj.name));
  // TODO: assign default privileges

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "create-role");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to create role:%s", pTrans->id, pCreate->name);

  SSdbRaw *pCommitRaw = mndRoleActionEncode(&obj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed at line %d to create role, since %s", obj.name, lino, tstrerror(code));
  }
  mndRoleFreeObj(&obj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateRoleReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0, lino = 0;
  SRoleObj      *pRole = NULL;
  SUserObj      *pOperUser = NULL;
  SUserObj      *pUser = NULL;
  SCreateRoleReq createReq = {0};

  if (tDeserializeSCreateRoleReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  if ((code = mndAcquireRole(pMnode, createReq.name, &pRole)) == 0) {
    if (createReq.ignoreExists) {
      mInfo("role:%s, already exist, ignore exist is set", createReq.name);
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_ROLE_ALREADY_EXIST);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_ROLE_NOT_EXIST) {
      // continue
    } else {
      goto _exit;
    }
  }
  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_NO_USER_FROM_CONN);
  }

  mInfo("role:%s, start to create by %s", createReq.name, pOperUser->user);

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_ROLE));

  if (createReq.name[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_INVALID_ROLE_FORMAT);
  }
  code = mndAcquireUser(pMnode, createReq.name, &pUser);
  if (pUser != NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_ALREADY_EXIST);
  }
  code = mndCreateRole(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char    detail[128] = {0};
  int32_t len = snprintf(detail, sizeof(detail), "operUser:%s", pOperUser->user);
  auditRecord(pReq, pMnode->clusterId, "createRole", "", createReq.name, detail, len);
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to create at line %d since %s", createReq.name, lino, tstrerror(code));
  }
  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  mndReleaseRole(pMnode, pRole);
  tFreeSCreateRoleReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndDropParentRole(SMnode *pMnode, SRoleObj *pChild) {
  return 0;
}

static int32_t mndDropRole(SMnode *pMnode, SRpcMsg *pReq, SRoleObj *pObj) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "drop-role");
  if (pTrans == NULL) {
    mError("role:%s, failed to drop since %s", pObj->name, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  mInfo("trans:%d, used to drop role:%s", pTrans->id, pObj->name);

  SSdbRaw *pCommitRaw = mndRoleActionEncode(pObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_CHECK_EXIT(mndDropParentRole(pMnode, pObj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropRoleReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = 0;
  int32_t      lino = 0;
  SRoleObj    *pObj = NULL;
  SDropRoleReq dropReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSDropRoleReq(pReq->pCont, pReq->contLen, &dropReq));

  mInfo("role:%s, start to drop", dropReq.name);
  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_ROLE));

  if (dropReq.name[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_INVALID_ROLE_FORMAT);
  }

  TAOS_CHECK_EXIT(mndAcquireRole(pMnode, dropReq.name, &pObj));

  TAOS_CHECK_EXIT(mndDropRole(pMnode, pReq, pObj));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "dropRole", "", dropReq.name, dropReq.sql, dropReq.sqlLen);
_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("role:%s, failed to drop at line %d since %s", dropReq.name, lino, tstrerror(code));
  }
  mndReleaseRole(pMnode, pObj);
  tFreeSDropRoleReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessGetRoleAuthReq(SRpcMsg *pReq) {

  TAOS_RETURN(0);
}

static int32_t mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0, lino = 0;
  int32_t   numOfRows = 0;
  SRoleObj *pObj = NULL;
  int32_t   cols = 0;
  int32_t   bufSize = TSDB_ROLE_MAX_CHILDREN * TSDB_ROLE_LEN + VARSTR_HEADER_SIZE;
  char     *buf = NULL;

  if (!(buf = taosMemoryMalloc(bufSize))) {
    TAOS_CHECK_EXIT(terrno);
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ROLE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_ROLE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pObj->name, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pObj, pShow->pIter, _exit);

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      int8_t enable = pObj->enable ? 1 : 0;
      COL_DATA_SET_VAL_GOTO((const char *)&enable, false, pObj, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      COL_DATA_SET_VAL_GOTO((const char *)&pObj->createdTime, false, pObj, pShow->pIter, _exit);
    }
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      COL_DATA_SET_VAL_GOTO((const char *)&pObj->updateTime, false, pObj, pShow->pIter, _exit);
    }

    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
      void  *pIter = NULL;
      size_t klen = 0, tlen = 0;
      char  *pBuf = POINTER_SHIFT(buf, VARSTR_HEADER_SIZE);
      while (pIter = taosHashIterate(pObj->children, pIter)) {
        char *roleName = taosHashGetKey(pIter, &klen);
        tlen += snprintf(pBuf + tlen, bufSize - tlen, "%s,", roleName);
      }
      if (tlen > 0) {
        pBuf[tlen - 1] = 0;  // remove last ','
      } else {
        pBuf[0] = 0;
      }
      varDataSetLen(buf, tlen);
      COL_DATA_SET_VAL_GOTO((const char *)buf, false, pObj, pShow->pIter, _exit);
    }
    numOfRows++;
    sdbRelease(pSdb, pObj);
  }
  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextRole(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}