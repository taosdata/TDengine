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

int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }

void mndCleanupPrivilege(SMnode *pMnode) {}

int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  switch (operType) {
    case MND_OPER_CONNECT:
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

int32_t mndCheckAlterUserPrivilege(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter) {
  if (pUser->superUser && pAlter->alterType != TSDB_ALTER_USER_PASSWD &&
      pAlter->alterType != TSDB_ALTER_USER_ADD_WHITE_LIST && pAlter->alterType != TSDB_ALTER_USER_DROP_WHITE_LIST) {
    TAOS_RETURN(TSDB_CODE_MND_NO_RIGHTS);
  }

  if (pOperUser->superUser) return 0;

  if (!pOperUser->enable) {
    TAOS_RETURN(TSDB_CODE_MND_USER_DISABLED);
  }

  if (pAlter->alterType == TSDB_ALTER_USER_PASSWD) {
    if (strcmp(pUser->user, pOperUser->user) == 0) {
      if (pOperUser->sysInfo) return 0;
    }
  }

  TAOS_RETURN(TSDB_CODE_MND_NO_RIGHTS);
}

int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, EShowType showType, const char *dbname) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

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
    code = mndCheckDbPrivilegeByName(pMnode, user, MND_OPER_READ_OR_WRITE_DB, dbname);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  TAOS_RETURN(code);
}

int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

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

int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *dbname) {
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

  code = mndCheckDbPrivilege(pMnode, user, operType, pDb);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

int32_t mndCheckViewPrivilege(SMnode *pMnode, const char *user, EOperType operType, const char *pViewFName) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

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

int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, EOperType operType, SMqTopicObj *pTopic) {
  int32_t   code = 0;
  SUserObj *pUser = NULL;

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, user, &pUser), NULL, _OVER);

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_DISABLED, NULL, _OVER);
  }

  if (operType == MND_OPER_SUBSCRIBE) {
    if (strcmp(pUser->user, pTopic->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->topics, pTopic->name, strlen(pTopic->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_CREATE_TOPIC) {
    if (mndCheckDbPrivilegeByName(pMnode, user, MND_OPER_READ_DB, pTopic->db) == 0) goto _OVER;
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

int32_t mndSetUserWhiteListRsp(SMnode *pMnode, SUserObj *pUser, SGetUserWhiteListRsp *pWhiteListRsp) {
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

int32_t mndSetUserWhiteListDualRsp(SMnode *pMnode, SUserObj *pUser, SGetUserWhiteListRsp *pWhiteListRsp) {
  if (tsEnableWhiteList) {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = pUser->pIpWhiteListDual->num;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteListsDual == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memcpy(pWhiteListRsp->pWhiteListsDual, pUser->pIpWhiteListDual->pIpRanges,
                 pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
  } else {
    (void)memcpy(pWhiteListRsp->user, pUser->user, TSDB_USER_LEN);
    pWhiteListRsp->numWhiteLists = 2;
    pWhiteListRsp->pWhiteListsDual = taosMemoryMalloc(pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    if (pWhiteListRsp->pWhiteLists == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    (void)memset(pWhiteListRsp->pWhiteListsDual, 0, pWhiteListRsp->numWhiteLists * sizeof(SIpRange));
    pWhiteListRsp->pWhiteListsDual[0].type = 0;  // ipv4
    pWhiteListRsp->pWhiteListsDual[1].type = 1;  // ipv6
  }
  TAOS_RETURN(0);

  return 0;
}

int32_t mndEnableIpWhiteList(SMnode *pMnode) { return 1; }
