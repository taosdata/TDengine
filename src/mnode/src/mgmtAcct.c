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
#include "mgmtDb.h"
#include "mgmtUser.h"
#ifndef _ACCOUNT

static SAcctObj tsAcctObj = {0};

int32_t acctInit() {
  tsAcctObj.acctId = 0;
  strcpy(tsAcctObj.user, "root");
  return TSDB_CODE_SUCCESS;
}

void      acctCleanUp() {}
SAcctObj *acctGetAcct(char *acctName) { return &tsAcctObj; }
void      acctIncRef(SAcctObj *pAcct) {}
void      acctDecRef(SAcctObj *pAcct) {}
int32_t   acctCheck(SAcctObj *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }
#endif

void acctAddDb(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = pAcct;
  acctIncRef(pAcct);
}

void acctRemoveDb(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = NULL;
  acctDecRef(pAcct);
}

void acctAddUser(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = pAcct;
  acctIncRef(pAcct);
}

void acctRemoveUser(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = NULL;
  acctDecRef(pAcct);
}