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
#include "mndUser.h"
#include "mndDb.h"

int32_t mndInitPrivilege(SMnode *pMnode) { return 0; }

void mndCleanupPrivilege(SMnode *pMnode) {}

int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, EOperType operType) {
  int32_t   code = 0;
  SUserObj *pUser = mndAcquireUser(pMnode, user);

  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
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

int32_t mndCheckShowPrivilege(SMnode *pMnode, const char *user, int32_t showType) {
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

  if (!pUser->sysInfo) {
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    code = -1;
    goto _OVER;
  }

  terrno = TSDB_CODE_MND_NO_RIGHTS;
  code = -1;

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

  if (operType == MND_OPER_ALTER_DB || operType == MND_OPER_DROP_DB || operType == MND_OPER_COMPACT_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0 && pUser->sysInfo) goto _OVER;
  }

  if (operType == MND_OPER_USE_DB) {
    if (strcmp(pUser->user, pDb->createUser) == 0) goto _OVER;
    if (taosHashGet(pUser->readDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
    if (taosHashGet(pUser->writeDbs, pDb->name, strlen(pDb->name) + 1) != NULL) goto _OVER;
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

int32_t mndCheckDbPrivilegeByName(SMnode *pMnode, const char *user, EOperType operType, const char *name) {
  SDbObj *pDb = mndAcquireDb(pMnode, name);
  if (pDb == NULL) return -1;

  int32_t code = mndCheckDbPrivilege(pMnode, user, operType, pDb);
  mndReleaseDb(pMnode, pDb);
  return code;
}