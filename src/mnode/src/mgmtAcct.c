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
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "tschemautil.h"

extern void *userSdb;
extern void *dbSdb;
SAcctObj     acctObj;

int mgmtGetAcctsNum();
SShowObj *mgmtGetNextAcct(SShowObj *pShow, SAcctObj **pAcct);

int mgmtAddDbIntoAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  pDb->next = pAcct->pHead;
  pDb->prev = NULL;

  if (pAcct->pHead) pAcct->pHead->prev = pDb;

  pAcct->pHead = pDb;
  pAcct->acctInfo.numOfDbs++;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtRemoveDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pDb->prev) pDb->prev->next = pDb->next;

  if (pDb->next) pDb->next->prev = pDb->prev;

  if (pDb->prev == NULL) pAcct->pHead = pDb->next;

  pAcct->acctInfo.numOfDbs--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtAddUserIntoAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  pUser->next = pAcct->pUser;
  pUser->prev = NULL;

  if (pAcct->pUser) pAcct->pUser->prev = pUser;

  pAcct->pUser = pUser;
  pAcct->acctInfo.numOfUsers++;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtRemoveUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pUser->prev) pUser->prev->next = pUser->next;

  if (pUser->next) pUser->next->prev = pUser->prev;

  if (pUser->prev == NULL) pAcct->pUser = pUser->next;

  pAcct->acctInfo.numOfUsers--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtAddConnIntoAcct(SConnObj *pConn) {
  SAcctObj *pAcct = pConn->pAcct;
  if (pAcct == NULL) return 0;

  pthread_mutex_lock(&pAcct->mutex);

  assert(pConn != pAcct->pConn);

  pConn->next = pAcct->pConn;
  pConn->prev = NULL;

  if (pAcct->pConn) pAcct->pConn->prev = pConn;

  pAcct->pConn = pConn;
  pAcct->acctInfo.numOfConns++;

  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtRemoveConnFromAcct(SConnObj *pConn) {
  SAcctObj *pAcct = pConn->pAcct;
  if (pAcct == NULL) return 0;

  pthread_mutex_lock(&pAcct->mutex);

  if (pConn->prev) pConn->prev->next = pConn->next;

  if (pConn->next) pConn->next->prev = pConn->prev;

  if (pConn->prev == NULL) pAcct->pConn = pConn->next;

  pAcct->acctInfo.numOfConns--;
  // pAcct->numOfUsers--;

  if (pConn->pQList) {
    pAcct->acctInfo.numOfQueries -= pConn->pQList->numOfQueries;
    pAcct->acctInfo.numOfStreams -= pConn->pSList->numOfStreams;
  }

  pthread_mutex_unlock(&pAcct->mutex);

  pConn->next = NULL;
  pConn->prev = NULL;

  return 0;
}

int32_t mgmtInitAcctsImp() { return 0; }
int32_t (*mgmtInitAccts)() = mgmtInitAcctsImp;

void mgmtCreateRootAcctImp() {}
void (*mgmtCreateRootAcct)() = mgmtCreateRootAcctImp;

SAcctObj *mgmtGetAcctImp(char *acctName) { return &acctObj; }
SAcctObj* (*mgmtGetAcct)(char *acctName) = mgmtGetAcctImp;

int32_t mgmtCheckUserLimitImp(SAcctObj *pAcct) {
  int numOfUsers = sdbGetNumOfRows(userSdb);
  if (numOfUsers >= tsMaxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d", numOfUsers, tsMaxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return 0;
}
int32_t (*mgmtCheckUserLimit)(SAcctObj *pAcct) = mgmtCheckUserLimitImp;

int32_t mgmtCheckDbLimitImp(SAcctObj *pAcct) {
  int numOfDbs = sdbGetNumOfRows(dbSdb);
  if (numOfDbs >= tsMaxDbs) {
    mWarn("numOfDbs:%d, exceed tsMaxDbs:%d", numOfDbs, tsMaxDbs);
    return TSDB_CODE_TOO_MANY_DATABSES;
  }
  return 0;
}
int32_t (*mgmtCheckDbLimit)(SAcctObj *pAcct) = mgmtCheckDbLimitImp;

int32_t mgmtCheckTableLimitImp(SAcctObj *pAcct, SCreateTableMsg *pCreate) { return 0; }
int32_t (*mgmtCheckTableLimit)(SAcctObj *pAcct, SCreateTableMsg *pCreate) = mgmtCheckTableLimitImp;

void mgmtCheckAcctImp() {
  SAcctObj *pAcct = &acctObj;
  pAcct->acctId = 0;
  strcpy(pAcct->user, "root");

  mgmtCreateUser(pAcct, "root", "taosdata");
  mgmtCreateUser(pAcct, "monitor", tsInternalPass);
  mgmtCreateUser(pAcct, "_root", tsInternalPass);
}
void (*mgmtCheckAcct)() = mgmtCheckAcctImp;

void mgmtCleanUpAcctsImp() {}
void (*mgmtCleanUpAccts)() = mgmtCleanUpAcctsImp;

int32_t mgmtGetAcctMetaImp(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) { return TSDB_CODE_OPS_NOT_SUPPORT; }
int32_t (*mgmtGetAcctMeta)(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) = mgmtGetAcctMetaImp;

int32_t mgmtRetrieveAcctsImp(SShowObj *pShow, char *data, int rows, SConnObj *pConn) { return 0; }
int32_t (*mgmtRetrieveAccts)(SShowObj *pShow, char *data, int rows, SConnObj *pConn) = mgmtRetrieveAcctsImp;



void mgmtAddMeterStatisticToAcct(SAcctObj *pAcct, int numOfColumns) {
  pAcct->acctInfo.numOfTimeSeries += (numOfColumns - 1);
}
