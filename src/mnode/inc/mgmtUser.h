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

#ifndef TDENGINE_MGMT_USER_H
#define TDENGINE_MGMT_USER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

int32_t   mgmtInitUsers();
SUserObj *mgmtGetUser(char *name);
int32_t   mgmtCreateUser(SAcctObj *pAcct, char *name, char *pass);
int32_t   mgmtDropUser(SAcctObj *pAcct, char *name);
int32_t   mgmtUpdateUser(SUserObj *pUser);
int32_t   mgmtGetUserMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t   mgmtRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn);
void      mgmtCleanUpUsers();
SUserObj *mgmtGetUserFromConn(void *pConn);

#ifdef __cplusplus
}
#endif

#endif
