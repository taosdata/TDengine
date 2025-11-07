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
#include "mndRole.h"
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

// clang-format on

#define ROLE_VER_NUMBER                      1
#define ROLE_RESERVE_SIZE                    63


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
  TAOS_CHECK_RETURN(ipWhiteMgtInit());

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
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
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

SSdbRaw *mndRoleActionEncode(SRoleObj *pRole) {
    return NULL;
}

static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw) {
  return NULL;
}

static int32_t mndRoleActionInsert(SSdb *pSdb, SRoleObj *pRole) {
  

  return 0;
}

int32_t mndRoleDupObj(SRoleObj *pRole, SRoleObj *pNew) { return 0; }

void mndRoleFreeObj(SRoleObj *pRole) {

}

static int32_t mndRoleActionDelete(SSdb *pSdb, SRoleObj *pRole) {
  return 0;
}

static int32_t mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew) {


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

static int32_t mndCreateRole(SMnode *pMnode, char *acct, SCreateRoleReq *pCreate, SRpcMsg *pReq) { return 0; }

static int32_t mndProcessCreateRoleReq(SRpcMsg *pReq) {
  

  TAOS_RETURN(0);
}


static int32_t mndDropRole(SMnode *pMnode, SRpcMsg *pReq, SRoleObj *pRole) {
  TAOS_RETURN(0);
}

static int32_t mndProcessDropRoleReq(SRpcMsg *pReq) {
  
  TAOS_RETURN(0);
}

static int32_t mndProcessGetRoleAuthReq(SRpcMsg *pReq) {

  TAOS_RETURN(0);
}

static int32_t mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }

static void mndCancelGetNextRole(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ROLE);
}