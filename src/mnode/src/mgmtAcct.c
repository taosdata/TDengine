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
#include "mnode.h"
#include "mgmtAcct.h"

static SAcctObj tsAcctObj = {0};

int32_t mgmtInitAccts() {
  tsAcctObj.acctId = 0;
  strcpy(tsAcctObj.user, "root");
  return TSDB_CODE_SUCCESS;
}

SAcctObj *mgmtGetAcct(char *acctName) { return &tsAcctObj; }

void mgmtCleanUpAccts() {}

int32_t mgmtAddDbIntoAcct(SAcctObj *pAcct, SDbObj *pDb) { return TSDB_CODE_SUCCESS; }

int32_t mgmtRemoveDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) { return TSDB_CODE_SUCCESS; }

int32_t mgmtAddUserIntoAcct(SAcctObj *pAcct, SUserObj *pUser) { return TSDB_CODE_SUCCESS; }

int32_t mgmtRemoveUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) { return TSDB_CODE_SUCCESS; }

int32_t mgmtCheckUserLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

int32_t mgmtCheckDbLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

int32_t mgmtCheckTableLimit(SAcctObj *pAcct) { return TSDB_CODE_SUCCESS; }

#endif