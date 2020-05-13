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

#include "tacct.h"

int32_t mgmtInitAccts();
void    mgmtCleanUpAccts();
void *  mgmtGetAcct(char *acctName);
void *  mgmtGetNextAcct(void *pIter, SAcctObj **pAcct);
void    mgmtIncAcctRef(SAcctObj *pAcct);
void    mgmtDecAcctRef(SAcctObj *pAcct);
void    mgmtAddDbToAcct(SAcctObj *pAcct, SDbObj *pDb);
void    mgmtDropDbFromAcct(SAcctObj *pAcct, SDbObj *pDb);
void    mgmtAddUserToAcct(SAcctObj *pAcct, SUserObj *pUser);
void    mgmtDropUserFromAcct(SAcctObj *pAcct, SUserObj *pUser);

#ifdef __cplusplus
}
#endif

#endif
