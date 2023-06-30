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
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
  }

  switch (operType) {
    case MND_OPER_CONNECT:
    case MND_OPER_CREATE_FUNC:
    case MND_OPER_DROP_FUNC:
    case MND_OPER_SHOW_VARIBALES:
      break;
    default:
      terrno = TSDB_CODE_MND_NO_RIGHTS;
      code = -1;
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckAlterUserPrivilege(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter) {
  if (pUser->superUser && pAlter->alterType != TSDB_ALTER_USER_PASSWD) {
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    return -1;
  }

  if (pOperUser->superUser) return 0;

  if (!pOperUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    return -1;
  }

  if (pAlter->alterType == TSDB_ALTER_USER_PASSWD) {
    if (strcmp(pUser->user, pOperUser->user) == 0) {
      if (pOperUser->sysInfo) return 0;
    }
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  return -1;
}

int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, EShowType showType, const char *dbname) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) {
    goto _OVER;
  }

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
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
      code = 0;
      break;
    default:
      terrno = TSDB_CODE_MND_NO_RIGHTS;
      code = -1;
      goto _OVER;
  }

  if (showType == TSDB_MGMT_TABLE_STB || showType == TSDB_MGMT_TABLE_VGROUP || showType == TSDB_MGMT_TABLE_INDEX) {
    code = mndCheckDbPrivilegeByName(pMnode, user, MND_OPER_READ_OR_WRITE_DB, dbname);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckDbPrivilege(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
  }

  if (operType == MND_OPER_CREATE_DB) {
    if (pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_ALTER_DB || operType == MND_OPER_DROP_DB || operType == MND_OPER_COMPACT_DB ||
      operType == MND_OPER_TRIM_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_USE_DB || operType == MND_OPER_READ_OR_WRITE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
    if (taosHashGet(pUser->useDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_WRITE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  if (operType == MND_OPER_READ_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *dbname) {
  SDbObj *pDb = mndAcquireDb(pMnode, dbname);
  if (pDb == NULL) return -1;

  int32_t code = mndCheckDbPrivilege(pMnode, user, operType, pDb);
  mndReleaseDb(pMnode, pDb);
  return code;
}

int32_t mndCheckTopicPrivilege(SMnode *pMnode, const char *user, EOperType operType, SMqTopicObj *pTopic) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    code = -1;
    goto _OVER;
  }

  if (pUser->superUser) goto _OVER;

  if (!pUser->enable) {
    terrno = TSDB_CODE_MND_USER_DISABLED;
    code = -1;
    goto _OVER;
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

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

_OVER:
  mndReleaseUser(pMnode, pUser);
  return code;
}

int32_t mndCheckTopicPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *topicName) {
  SMqTopicObj *pTopic = mndAcquireTopic(pMnode, (char *)topicName);
  if (pTopic == NULL) return -1;

  int32_t code = mndCheckTopicPrivilege(pMnode, user, operType, pTopic);
  mndReleaseTopic(pMnode, pTopic);
  return code;
}

int32_t mndSetUserAuthRsp(SMnode *pMnode, SUserObj *pUser, SGetUserAuthRsp *pRsp) {
  memcpy(pRsp->user, pUser->user, TSDB_USER_LEN);
  pRsp->superAuth = pUser->superUser;
  pRsp->version = pUser->authVersion;
  pRsp->passVer = pUser->passVersion;
  pRsp->enable = pUser->enable;
  pRsp->sysInfo = pUser->sysInfo;
  taosRLockLatch(&pUser->lock);
  pRsp->readDbs = mndDupDbHash(pUser->readDbs);
  pRsp->writeDbs = mndDupDbHash(pUser->writeDbs);
  pRsp->readTbs = mndDupTableHash(pUser->readTbs);
  pRsp->writeTbs = mndDupTableHash(pUser->writeTbs);
  pRsp->useDbs = mndDupTableHash(pUser->useDbs);
  taosRUnLockLatch(&pUser->lock);
  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == pRsp->createdDbs) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;

    if (strcmp(pDb->createUser, pUser->user) == 0) {
      int32_t len = strlen(pDb->name) + 1;
      taosHashPut(pRsp->createdDbs, pDb->name, len, pDb->name, len);
    }

    sdbRelease(pSdb, pDb);
  }

  return 0;
}
