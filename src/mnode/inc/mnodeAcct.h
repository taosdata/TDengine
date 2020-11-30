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

#ifndef TDENGINE_MNODE_ACCT_H
#define TDENGINE_MNODE_ACCT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tacct.h"

int32_t mnodeInitAccts();
void    mnodeCleanupAccts();
void    mnodeGetStatOfAllAcct(SAcctInfo* pAcctInfo);
void *  mnodeGetAcct(char *acctName);
void *  mnodeGetNextAcct(void *pIter, SAcctObj **pAcct);
void    mnodeCancelGetNextAcct(void *pIter);
void    mnodeIncAcctRef(SAcctObj *pAcct);
void    mnodeDecAcctRef(SAcctObj *pAcct);
void    mnodeAddDbToAcct(SAcctObj *pAcct, SDbObj *pDb);
void    mnodeDropDbFromAcct(SAcctObj *pAcct, SDbObj *pDb);
void    mnodeAddUserToAcct(SAcctObj *pAcct, SUserObj *pUser);
void    mnodeDropUserFromAcct(SAcctObj *pAcct, SUserObj *pUser);

#ifdef __cplusplus
}
#endif

#endif
