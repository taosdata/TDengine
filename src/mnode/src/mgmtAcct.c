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

static SAcctObj tsAcctObj = {0};

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

  if (pUser->next) {
    pUser->next->prev = pUser->prev;
  }

  if (pUser->prev == NULL) {
    pAcct->pUser = pUser->next;
  }

  pAcct->acctInfo.numOfUsers--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

#ifndef _ACCOUNT

int32_t mgmtInitAccts() {
  tsAcctObj.acctId = 0;
  strcpy(tsAcctObj.user, "root");
  return TSDB_CODE_SUCCESS;
}

SAcctObj *mgmtGetAcct(char *acctName) { return &tsAcctObj; }

void mgmtCleanUpAccts() {}

int32_t mgmtCheckUserLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

int32_t mgmtCheckDbLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

int32_t mgmtCheckTableLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

#endif