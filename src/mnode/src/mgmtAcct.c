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
#include "taoserror.h"
#include "mnode.h"
#include "mgmtAcct.h"
#ifndef _ACCOUNT

static SAcctObj tsAcctObj = {0};

int32_t acctInit() {
  tsAcctObj.acctId = 0;
  strcpy(tsAcctObj.user, "root");
  return TSDB_CODE_SUCCESS;
}

void      acctCleanUp() {}
SAcctObj *acctGetAcct(char *acctName) { return &tsAcctObj; }
int32_t   acctCheck(SAcctObj *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }
#endif

void acctAddDb(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  pDb->next = pAcct->pHead;
  pDb->prev = NULL;
  pDb->pAcct = pAcct;

  if (pAcct->pHead) {
    pAcct->pHead->prev = pDb;
  }

  pAcct->pHead = pDb;
  pAcct->acctInfo.numOfDbs++;
  pthread_mutex_unlock(&pAcct->mutex);
}

void acctRemoveDb(SAcctObj *pAcct, SDbObj *pDb) {
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
}

void acctAddUser(SAcctObj *pAcct, SUserObj *pUser) {
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
}

void acctRemoveUser(SAcctObj *pAcct, SUserObj *pUser) {
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
}