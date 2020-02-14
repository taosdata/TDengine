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

int32_t mgmtCreateAcct(char *name, char *pass, SAcctCfg *pCfg);
int32_t mgmtUpdateAcct(SAcctObj *pAcct);
int32_t mgmtDropAcct(char *name);
int32_t mgmtAddDbIntoAcct(SAcctObj *pAcct, SDbObj *pDb);
int32_t mgmtRemoveDbFromAcct(SAcctObj *pAcct, SDbObj *pDb);
int32_t mgmtAddUserIntoAcct(SAcctObj *pAcct, SUserObj *pUser);
int32_t mgmtRemoveUserFromAcct(SAcctObj *pAcct, SUserObj *pUser);
int32_t mgmtAddConnIntoAcct(SConnObj *pConn);
int32_t mgmtRemoveConnFromAcct(SConnObj *pConn);
int32_t mgmtAlterAcct(char *name, char *pass, SAcctCfg *pCfg);
int64_t mgmtGetAcctStatistic(SAcctObj *pAcct);

extern int32_t   (*mgmtInitAccts)();
extern SAcctObj* (*mgmtGetAcct)(char *acctName);
extern void      (*mgmtCreateRootAcct)();
extern int32_t   (*mgmtCheckUserLimit)(SAcctObj *pAcct);
extern int32_t   (*mgmtCheckDbLimit)(SAcctObj *pAcct);
extern int32_t   (*mgmtCheckTableLimit)(SAcctObj *pAcct, SCreateTableMsg *pCreate);
extern void      (*mgmtCheckAcct)();
extern void      (*mgmtCleanUpAccts)();
extern int32_t   (*mgmtGetAcctMeta)(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);
extern int32_t   (*mgmtRetrieveAccts)(SShowObj *pShow, char *data, int32_t rows, SConnObj *pConn);



void mgmtAddMeterStatisticToAcct(SAcctObj *pAcct, int numOfColumns);

#ifdef __cplusplus
}
#endif

#endif
