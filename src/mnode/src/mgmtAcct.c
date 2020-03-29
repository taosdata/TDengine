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
#ifndef _ACCOUNT
#include "os.h"
#include "taoserror.h"
#include "mnode.h"
#include "mgmtAcct.h"

static SAcctObj tsAcctObj = {0};

int32_t acctInit() {
  tsAcctObj.acctId = 0;
  strcpy(tsAcctObj.user, "root");
  return TSDB_CODE_SUCCESS;
}

void      acctCleanUp() {}
SAcctObj *acctGetAcct(char *acctName) { return &tsAcctObj; }
int32_t   acctCheck(SAcctObj *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }

int32_t acctAddDb(SAcctObj *pAcct, SDbObj *pDb) { return TSDB_CODE_SUCCESS; }
int32_t acctRemoveDb(SAcctObj *pAcct, SDbObj *pDb) { return TSDB_CODE_SUCCESS; }
int32_t acctAddUser(SAcctObj *pAcct, SUserObj *pUser) { return TSDB_CODE_SUCCESS; }
int32_t acctRemoveUser(SAcctObj *pAcct, SUserObj *pUser) { return TSDB_CODE_SUCCESS; }

#endif