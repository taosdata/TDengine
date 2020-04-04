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

#ifndef TDENGINE_MGMT_ACCT_H
#define TDENGINE_MGMT_ACCT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnode.h"

typedef enum {
  TSDB_ACCT_USER,
  TSDB_ACCT_DB,
  TSDB_ACCT_TABLE
} EAcctGrantType;

int32_t   acctInit();
void      acctCleanUp();
SAcctObj *acctGetAcct(char *acctName);
void      acctIncRef(SAcctObj *pAcct);
void      acctDecRef(SAcctObj *pAcct);
int32_t   acctCheck(SAcctObj *pAcct, EAcctGrantType type);

void      acctAddDb(SAcctObj *pAcct, SDbObj *pDb);
void      acctRemoveDb(SAcctObj *pAcct, SDbObj *pDb);
void      acctAddUser(SAcctObj *pAcct, SUserObj *pUser);
void      acctRemoveUser(SAcctObj *pAcct, SUserObj *pUser);

#ifdef __cplusplus
}
#endif

#endif
