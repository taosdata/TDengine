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

struct _acct_obj;
struct _user_obj;
struct _db_obj;
 
int32_t mgmtInitAccts();
void    mgmtCleanUpAccts();
void   *mgmtGetAcct(char *acctName);
void    mgmtIncAcctRef(struct _acct_obj *pAcct);
void    mgmtDecAcctRef(struct _acct_obj *pAcct);

void    mgmtAddDbToAcct(struct _acct_obj *pAcct, struct _db_obj *pDb);
void    mgmtDropDbFromAcct(struct _acct_obj *pAcct, struct _db_obj *pDb);
void    mgmtAddUserToAcct(struct _acct_obj *pAcct, struct _user_obj *pUser);
void    mgmtDropUserFromAcct(struct _acct_obj *pAcct, struct _user_obj *pUser);

#ifdef __cplusplus
}
#endif

#endif
