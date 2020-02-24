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
#include "os.h"
#include "tschemautil.h"
#include "taoserror.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtTable.h"
#include "mgmtUser.h"

extern void *tsUserSdb;
extern void *tsDbSdb;
static SAcctObj tsAcctObj;

int32_t mgmtAddDbIntoAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  pDb->next = pAcct->pHead;
  pDb->prev = NULL;

  if (pAcct->pHead) {
    pAcct->pHead->prev = pDb;
  }

  pAcct->pHead = pDb;
  pAcct->acctInfo.numOfDbs++;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtRemoveDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pDb->prev) {
    pDb->prev->next = pDb->next;
  }

  if (pDb->next) {
    pDb->next->prev = pDb->prev;
  }

  if (pDb->prev == NULL) {
    pAcct->pHead = pDb->next;
  }

  pAcct->acctInfo.numOfDbs--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtAddUserIntoAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  pUser->next = pAcct->pUser;
  pUser->prev = NULL;

  if (pAcct->pUser) {
    pAcct->pUser->prev = pUser;
  }

  pAcct->pUser = pUser;
  pAcct->acctInfo.numOfUsers++;
  pUser->pAcct = pAcct;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtRemoveUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pUser->prev) {
    pUser->prev->next = pUser->next;
  }

  if (pUser->next) pUser->next->prev = pUser->prev;

  if (pUser->prev == NULL) {
    pAcct->pUser = pUser->next;
  }

  pAcct->acctInfo.numOfUsers--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtInitAcctsImp() {
  SAcctObj *pAcct = &tsAcctObj;
  pAcct->acctId = 0;
  strcpy(pAcct->user, "root");
  return 0;
}

int32_t (*mgmtInitAccts)() = mgmtInitAcctsImp;

static SAcctObj *mgmtGetAcctImp(char *acctName) {
  return &tsAcctObj;
}

SAcctObj *(*mgmtGetAcct)(char *acctName) = mgmtGetAcctImp;

static int32_t mgmtCheckUserLimitImp(SAcctObj *pAcct) {
  int32_t numOfUsers = sdbGetNumOfRows(tsUserSdb);
  if (numOfUsers >= tsMaxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d", numOfUsers, tsMaxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return 0;
}

int32_t (*mgmtCheckUserLimit)(SAcctObj *pAcct) = mgmtCheckUserLimitImp;

static int32_t mgmtCheckDbLimitImp(SAcctObj *pAcct) {
  int32_t numOfDbs = sdbGetNumOfRows(tsDbSdb);
  if (numOfDbs >= tsMaxDbs) {
    mWarn("numOfDbs:%d, exceed tsMaxDbs:%d", numOfDbs, tsMaxDbs);
    return TSDB_CODE_TOO_MANY_DATABASES;
  }
  return 0;
}

int32_t (*mgmtCheckDbLimit)(SAcctObj *pAcct) = mgmtCheckDbLimitImp;

static int32_t mgmtCheckTableLimitImp(SAcctObj *pAcct, SCreateTableMsg *pCreate) {
  return 0;
}

int32_t (*mgmtCheckTableLimit)(SAcctObj *pAcct, SCreateTableMsg *pCreate) = mgmtCheckTableLimitImp;

static void mgmtCleanUpAcctsImp() {
}

void (*mgmtCleanUpAccts)() = mgmtCleanUpAcctsImp;

static int32_t mgmtGetAcctMetaImp(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t (*mgmtGetAcctMeta)(STableMeta *pMeta, SShowObj *pShow, void *pConn) = mgmtGetAcctMetaImp;

static int32_t mgmtRetrieveAcctsImp(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  return 0;
}

int32_t (*mgmtRetrieveAccts)(SShowObj *pShow, char *data, int32_t rows, void *pConn) = mgmtRetrieveAcctsImp;

SAcctObj *mgmtGetAcctFromConn(void *pConn) {
  SRpcConnInfo connInfo;
  rpcGetConnInfo(pConn, &connInfo);
  SUserObj *pUser =  mgmtGetUser(connInfo.user);
  if(pUser != NULL) {
    return pUser->pAcct;
  }

  return NULL;
}
