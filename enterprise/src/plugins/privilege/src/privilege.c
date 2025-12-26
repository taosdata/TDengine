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
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndTopic.h"
#include "mndUser.h"
#include "mndDef.h"


static bool mndMustChangePassword(SUserObj* pUser) {
  if (pUser->changePass == 1) {
    return true;
  }

  if (pUser->passwordLifeTime == -1) {
    return false;
  }

  int32_t age = taosGetTimestampSec() - pUser->passwords[0].setTime;
  return age >= pUser->passwordLifeTime;
}



int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }

void mndCleanupPrivilege(SMnode *pMnode) {}



int32_t mndCheckConnectPrivilege(SMnode *pMnode, SUserObj *pUser, const char* token, const SLoginInfo *li) {
  if ((!pUser->superUser) && (!pUser->enable)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  int64_t          now = taosGetTimestampSec();

  if (token == NULL && pUser->passwordLifeTime > 0 && pUser->passwordGraceTime >= 0) {
    int32_t age = now - pUser->passwords[0].setTime;
    int32_t maxLifeTime = pUser->passwordLifeTime + pUser->passwordGraceTime;
    if (age >= maxLifeTime) {
      return TSDB_CODE_MND_USER_PASSWORD_EXPIRED;
    }
  }

  if (!isTimeInDateTimeWhiteList(pUser->pTimeWhiteList, now)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  if (pUser->inactiveAccountTime >= 0 && (now - li->lastLoginTime >= pUser->inactiveAccountTime)) {
    return TSDB_CODE_MND_USER_DISABLED;
  }

  if (token == NULL && pUser->failedLoginAttempts >= 0 && li->failedLoginCount >= pUser->failedLoginAttempts) {
    if(pUser->passwordLockTime < 0 || now - li->lastFailedLoginTime < pUser->passwordLockTime) {
      return TSDB_CODE_MND_USER_DISABLED;
    }
  }

  // this function is implemented in mndProfile.c
  int32_t mndCountUserConns(SMnode *pMnode, const char *user);

  if (pUser->sessionPerUser >= 0) {
    int32_t currentSessions = mndCountUserConns(pMnode, pUser->user);
    if (currentSessions >= pUser->sessionPerUser) {
      return TSDB_CODE_MND_TOO_MANY_CONNECTIONS;
    }
  }

  return 0;
}



int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  switch (operType) {
    case MND_OPER_CREATE_FUNC:
    case MND_OPER_DROP_FUNC:
    case MND_OPER_SHOW_VARIABLES:
      break;
    default:
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}



static bool canChangePassword(SUserObj *pOperUser, SUserObj *pUser) {
  if (pOperUser->superUser) {
    return true;
  }

  if (!pOperUser->enable) {
    return false;
  }

  if (strcmp(pUser->user, pOperUser->user) != 0) {
    return false;
  }

  if (pUser->changePass == 0) {
    return false;
  }

  if (pUser->passwordLifeTime == -1 || pUser->passwordGraceTime == -1) {
    return true;
  }

  int32_t age = taosGetTimestampSec() - pUser->passwords[0].setTime;
  int32_t maxLifeTime = pUser->passwordLifeTime + pUser->passwordGraceTime;
  return age < maxLifeTime;
}



int32_t mndCheckAlterUserPrivilege(SMnode* pMnode, const char *opUser, const char* opToken, SUserObj *pUser, SAlterUserReq *pAlter) {
  int32_t code = 0, lino = 0;
  SUserObj *pOperUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, opUser, &pOperUser), &lino, _OVER);

  if (pAlter->alterType != TSDB_ALTER_USER_BASIC_INFO) {
    if (opToken == NULL && mndMustChangePassword(pOperUser)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
    }
    if (pOperUser->superUser && !pUser->superUser) {
      goto _OVER;
    }
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, &lino, _OVER);
  }

  if (pAlter->hasPassword) {
    if (!canChangePassword(pOperUser, pUser)) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  } else if (opToken == NULL && mndMustChangePassword(pOperUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, &lino, _OVER);
  }

  if (pOperUser->superUser) {
    if (!pUser->superUser) {
      // super user can alter any non-super user
      goto _OVER;
    }
  } else if (!pOperUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, &lino, _OVER);
  } else if (strcmp(pUser->user, pOperUser->user) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
  } else if (pAlter->numIpRanges > 0 || pAlter->numDropIpRanges > 0) {
    // user can not alter its own ip white list
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
  }

  // now there are two cases left:
  // 1. both pOperUser and pUser are superuser
  // 2. pOperUser and pUser are same user

  if (pAlter->hasEnable || pAlter->hasSysinfo || pAlter->hasCreatedb ||
      pAlter->hasChangepass || pAlter->hasSessionPerUser ||
      pAlter->hasConnectTime || pAlter->hasConnectIdleTime ||
      pAlter->hasCallPerSession || pAlter->hasVnodePerCall ||
      pAlter->hasFailedLoginAttempts || pAlter->hasPasswordLifeTime ||
      pAlter->hasPasswordReuseTime || pAlter->hasPasswordReuseMax ||
      pAlter->hasPasswordLockTime || pAlter->hasPasswordGraceTime ||
      pAlter->hasInactiveAccountTime || pAlter->hasAllowTokenNum ||
      pAlter->numTimeRanges > 0 || pAlter->numDropTimeRanges > 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
  }

  // super user can alter totp seed of any user, user can also alter its own totp seed
  // so no need to check pAlter->hasTotpseed here
_OVER:
  mndReleaseUser(pMnode, pOperUser);
  return code;
}



int32_t mndCheckTokenPrivilege(SMnode* pMnode, const char* opUser, const char* opToken, const char *user, const char* token) {
  int32_t   code = 0;
  SUserObj *pOperUser = NULL;

  if (opToken != NULL && token != NULL && taosStrcasecmp(opToken, token) == 0) {
    return TSDB_CODE_MND_NO_RIGHTS; // token cannot alter/drop itself
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, opUser, &pOperUser), NULL, _OVER);

  if (opToken == NULL && mndMustChangePassword(pOperUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

  if (pOperUser->superUser) {
    goto _OVER;
  }

  if (!pOperUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (strcmp(pOperUser->user, user) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pOperUser);
  TAOS_RETURN(code);
}



int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, const char* token, EShowType showType, const char *dbname) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (pUser->sysInfo) {
    goto _OVER;
  }

  switch (showType) {
    case TSDB_MGMT_TABLE_DB:
    case TSDB_MGMT_TABLE_STB:
    case TSDB_MGMT_TABLE_INDEX:
    case TSDB_MGMT_TABLE_STREAMS:
    case TSDB_MGMT_TABLE_CONSUMERS:
    case TSDB_MGMT_TABLE_TOPICS:
    case TSDB_MGMT_TABLE_SUBSCRIPTIONS:
    case TSDB_MGMT_TABLE_FUNC:
    case TSDB_MGMT_TABLE_QUERIES:
    case TSDB_MGMT_TABLE_CONNS:
    case TSDB_MGMT_TABLE_APPS:
    case TSDB_MGMT_TABLE_TRANS:
    case TSDB_MGMT_TABLE_COL:
    case TSDB_MGMT_TABLE_ANODE:
    case TSDB_MGMT_TABLE_ANODE_FULL:
      break;
    default:
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, NULL, _OVER);
  }

  if (showType == TSDB_MGMT_TABLE_STB || showType == TSDB_MGMT_TABLE_VGROUP || showType == TSDB_MGMT_TABLE_INDEX) {
    code = mndCheckDbPrivilegeByName(pMnode, user, token, MND_OPER_READ_OR_WRITE_DB, dbname);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, SDbObj *pDb) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (operType == MND_OPER_CREATE_DB) {
    if (pUser->createdb) goto _OVER;
  }

  if (operType == MND_OPER_ALTER_DB || operType == MND_OPER_COMPACT_DB || operType == MND_OPER_TRIM_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  } else if (operType == MND_OPER_DROP_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;  // TS-7279
  }

  if (operType == MND_OPER_USE_DB || operType == MND_OPER_READ_OR_WRITE_DB) {
    if (pDb != NULL) {
      if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
      if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
      if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
      if (taosHashGet(pUser->useDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
    } else {
      goto _OVER;
    }
  }

  if (operType == MND_OPER_WRITE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_READ_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, const char* token, EOperType operType, const char *dbname) {
  int32_t code = 0;

  const char *realDbName = NULL;
  const char *dot = strchr(dbname, '.');
  if (dot != NULL && *(dot + 1) != '\0') {
    realDbName = dot + 1;
  }

  if ((0 == strcasecmp(realDbName, TSDB_INFORMATION_SCHEMA_DB) ||
       (0 == strcasecmp(realDbName, TSDB_PERFORMANCE_SCHEMA_DB)))) {
    if (operType == MND_OPER_READ_DB) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_MND_NO_RIGHTS;
    }
  }

  SDbObj *pDb = mndAcquireDb(pMnode, dbname);

  if (pDb == NULL) {
    TAOS_RETURN(terrno);
  }

  code = mndCheckDbPrivilege(pMnode, user, token, operType, pDb);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndCheckStbPrivilege(SMnode *pMnode, SUserObj *pUser, const char* token, EOperType operType, SStbObj *pStb) {
  int32_t code = 0, lino = 0;
  SDbObj *pDb = NULL;

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_PASSWORD_EXPIRED);
  }

  if (pUser->superUser) goto _exit;

  if (!pUser->enable) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_USER_DISABLED);
  }

  if (!(pDb = mndAcquireDb(pMnode, pStb->db))) {
    code = terrno ? terrno : TSDB_CODE_MND_DB_NOT_EXIST;
    TAOS_CHECK_EXIT(code);
  }

  if (operType == MND_OPER_SHOW_STB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _exit;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _exit;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _exit;
    if (taosHashGet(pUser->readTbs, pStb->name, strlen(pStb->name) + 1) != NULL) goto _exit;
    if (taosHashGet(pUser->writeTbs, pStb->name, strlen(pStb->name) + 1) != NULL) goto _exit;
    if (taosHashGet(pUser->alterTbs, pStb->name, strlen(pStb->name) + 1) != NULL) goto _exit;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_exit:
  if (pDb) mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndCheckViewPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, const char *pViewFName) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (operType == MND_OPER_CREATE_VIEW || operType == MND_OPER_DROP_VIEW) {
    if (taosHashGet(pUser->alterViews, pViewFName, strlen(pViewFName) + 1) != NULL) goto _OVER;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, const char* token, EOperType operType, SMqTopicObj *pTopic) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (token == NULL && mndMustChangePassword(pUser)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_PASSWORD_EXPIRED, NULL, _OVER);
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (operType == MND_OPER_SUBSCRIBE) {
    if (strcmp(pUser->user, pTopic->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->topics, pTopic->name, strlen(pTopic->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_CREATE_TOPIC) {
    if (mndCheckDbPrivilegeByName(pMnode, user, token, MND_OPER_READ_DB, pTopic->db) == 0) goto _OVER;
  }

  if (operType == MND_OPER_DROP_TOPIC) {
    if (strcmp(pUser->user, pTopic->createUser) == 0) goto _OVER;
  }

  code = TSDB_CODE_MND_NO_RIGHTS;

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  int32_t code = 0;

  (void)memcpy(pRsp->user, pUser->user, TSDB_USER_LEN);
  pRsp->superAuth = pUser->superUser;
  pRsp->version = pUser->authVersion;
  pRsp->passVer = pUser->passVersion;
  pRsp->whiteListVer = pMnode->ipWhiteVer;
  pRsp->timeWhiteListVer = pMnode->timeWhiteVer;
  pRsp->enable = pUser->enable;
  pRsp->sysInfo = pUser->sysInfo;

  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == pRsp->createdDbs) {
    TAOS_RETURN(terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY);
  }

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;

    if (strcmp(pDb->createUser, pUser->user) == 0) {
      int32_t len = strlen(pDb->name) + 1;
      if ((code = taosHashPut(pRsp->createdDbs, pDb->name, len, pDb->name, len)) != 0) {
        sdbRelease(pSdb, pDb);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pDb);
  }

  taosRLockLatch(&pUser->lock);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->readDbs, &pRsp->readDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->writeDbs, &pRsp->writeDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readTbs, &pRsp->readTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeTbs, &pRsp->writeTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterTbs, &pRsp->alterTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readViews, &pRsp->readViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeViews, &pRsp->writeViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterViews, &pRsp->alterViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->useDbs, &pRsp->useDbs), NULL, _OVER);

_OVER:
  taosRUnLockLatch(&pUser->lock);
  TAOS_RETURN(code);
}

int32_t mndSetUserIpWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  if (tsEnableWhiteList) {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = pUser->pIpWhiteListDual->num;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    int32_t ipv4Count = 0;
    for (int32_t i = 0; i < pUser->pIpWhiteListDual->num; i++) {
      SIpRange *pRange = &pUser->pIpWhiteListDual->pIpRanges[i];
      if (pRange->type == 0) {
        memcpy(&pWhiteListRsp->pWhiteLists[ipv4Count], pRange, sizeof(SIpV4Range));
        ipv4Count++;
      }
    }
    pWhiteListRsp->numWhiteLists = ipv4Count;

  } else {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = 1;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memset(pWhiteListRsp->pWhiteLists, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpV4Range));
  }
  TAOS_RETURN(0);
}

int32_t mndSetUserIpWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserIpWhiteListRsp *pWhiteListRsp) {
  (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);

  if (tsEnableWhiteList) {
    pWhiteListRsp->numWhiteLists = pUser->pIpWhiteListDual->num;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteListsDual == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pWhiteListRsp->pWhiteListsDual, pUser->pIpWhiteListDual->pIpRanges,
                 pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
  } else {
    pWhiteListRsp->numWhiteLists = 2;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteListsDual == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memset(pWhiteListRsp->pWhiteListsDual, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    pWhiteListRsp->pWhiteListsDual[0].type = 0;  // ipv4
    pWhiteListRsp->pWhiteListsDual[1].type = 1;  // ipv6
  }
  TAOS_RETURN(0);
}

int32_t mndEnableIpWhiteList(SMnode *pMnode) { return 1; }
int32_t mndEnableTimeWhiteList(SMnode *pMnode) { return 1; }


int32_t mndSetUserDateTimeWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SUserDateTimeWhiteList *pWhiteListRsp) {
  (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);

  if (tsEnableWhiteList) {
    pWhiteListRsp->ver = pUser->timeWhiteListVer;
    pWhiteListRsp->numWhiteLists = pUser->pTimeWhiteList->num;
    pWhiteListRsp->pWhiteLists = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SDateTimeWhiteListItem));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pWhiteListRsp->pWhiteLists, pUser->pTimeWhiteList->ranges, pWhiteListRsp->numWhiteLists * sizeof(SDateTimeWhiteListItem));
  } else {
    pWhiteListRsp->ver = 0;
    pWhiteListRsp->numWhiteLists = 0;
  }

  TAOS_RETURN(0);
}