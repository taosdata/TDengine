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

#ifndef TDENGINE_ACCT_H
#define TDENGINE_ACCT_H

#ifdef __cplusplus
extern "C" {
#endif

struct _acct_obj;
struct _user_obj;
struct _db_obj;
 
typedef enum {
  TSDB_ACCT_USER,
  TSDB_ACCT_DB,
  TSDB_ACCT_TABLE
} EAcctGrantType;

int32_t acctInit();
void    acctCleanUp();
void   *acctGetAcct(char *acctName);
void    acctIncRef(struct _acct_obj *pAcct);
void    acctReleaseAcct(struct _acct_obj *pAcct);
int32_t acctCheck(struct _acct_obj *pAcct, EAcctGrantType type);

void    acctAddDb(struct _acct_obj *pAcct, struct _db_obj *pDb);
void    acctRemoveDb(struct _acct_obj *pAcct, struct _db_obj *pDb);
void    acctAddUser(struct _acct_obj *pAcct, struct _user_obj *pUser);
void    acctRemoveUser(struct _acct_obj *pAcct, struct _user_obj *pUser);

#ifdef __cplusplus
}
#endif

#endif
