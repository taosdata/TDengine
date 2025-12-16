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
#include "mndDb.h"
#include "mndUser.h"

#ifndef _PRIVILEGE
int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }
void    mndCleanupPrivilege(SMnode *pMnode) {}
bool    mndMustChangePassword(SUserObj* pUser) { return false; }


int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if ((!pUser->superUser) && (!pUser->enable)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckSysObjPrivilege(SMnode *pMnode, SUserObj *pUser, EPrivType privType, const char *owner,
                                const char *objFName, const char *tbName) {
  return 0;
}
int32_t mndCheckObjPrivilegeRec(SMnode *pMnode, SUserObj *pUser, EPrivType privType, const char *owner, int32_t acctId,
                                const char *objName, const char *tbName) {
  return 0;
}
int32_t mndCheckObjPrivilegeRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, const char *owner,
                                 const char *objFName, const char *tbName) {
  return 0;
}

int32_t mndCheckAlterUserPrivilege(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter) { return 0; }
int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, EShowType showType, const char *dbname) { return 0; }
int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb) { return 0; }
int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *dbname) {
  return 0;
}
int32_t mndCheckDbPrivilegeByNameRecF(SMnode *pMnode, SUserObj *pUser, EPrivType privType, const char *objFName,
                                      const char *tbName, char *outOwner) {
  return 0;
}
int32_t mndCheckStbPrivilege(SMnode *pMnode, SUserObj *pUser, EOperType operType, SStbObj *pStb) { return 0; }
int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, EOperType operType, SMqTopicObj *pTopic) { return 0; }

int32_t mndSetUserIpWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  int32_t code = 0;
  memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);

  pWhiteListRsp->numWhiteLists = 2;
  pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
  if (pWhiteListRsp->pWhiteListsDual == NULL) {
    return terrno;
  }
  memset(pWhiteListRsp->pWhiteListsDual, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
  pWhiteListRsp->pWhiteListsDual[0].type = 0;  //
  pWhiteListRsp->pWhiteListsDual[1].type = 1;

  return 0;
}

int32_t mndSetUserDateTimeWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SUserDateTimeWhiteList *pWhiteListRsp) {
  (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
  pWhiteListRsp->ver = 0;
  pWhiteListRsp->numWhiteLists = 0;
  TAOS_RETURN(0);
}

int32_t mndSetUserIpWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  int32_t code = 0;
  memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
  pWhiteListRsp->numWhiteLists = 1;

  SIpWhiteList *pWhiteList = NULL;
  code = cvtIpWhiteListDualToV4(pUser->pIpWhiteListDual, &pWhiteList);
  if (code != 0) {
    return code;
  }
  pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteList->num * sizeof(SIpV4Range));
  if (pWhiteListRsp->pWhiteLists == NULL) {
    taosMemoryFreeClear(pWhiteList);
    return terrno;
  }
  memset(pWhiteListRsp->pWhiteLists, 0, pWhiteList->num * sizeof(SIpV4Range));
  taosMemoryFreeClear(pWhiteList);
  return code;
}

int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  memcpy(pRsp->user, pUser->user, TSDB_USER_LEN);
  pRsp->superAuth = 1;
  pRsp->enable = pUser->enable;
  pRsp->sysInfo = pUser->sysInfo;
  pRsp->version = pUser->authVersion;
  pRsp->passVer = pUser->passVersion;
  pRsp->whiteListVer = pMnode->ipWhiteVer;

  SUserSessCfg sessCfg = {.sessPerUser = pUser->sessionPerUser,
                          .sessConnTime = pUser->connectTime,
                          .sessConnIdleTime = pUser->connectIdleTime,
                          .sessMaxConcurrency = pUser->callPerSession,
                          .sessMaxCallVnodeNum = pUser->vnodePerCall};
  memcpy(&pRsp->sessCfg, &sessCfg, sizeof(SUserSessCfg));
  pRsp->timeWhiteListVer = pMnode->timeWhiteVer;
  return 0;
}

int32_t mndEnableIpWhiteList(SMnode *pMnode) { return 0; }
int32_t mndEnableTimeWhiteList(SMnode *pMnode) { return 0; }

int32_t mndFetchIpWhiteList(SIpWhiteList *ipList, char **buf) {
  *buf = NULL;
  return 0;
}
#endif
