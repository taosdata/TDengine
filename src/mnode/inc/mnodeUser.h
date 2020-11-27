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

#ifndef TDENGINE_MNODE_USER_H
#define TDENGINE_MNODE_USER_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnodeDef.h"

int32_t   mnodeInitUsers();
void      mnodeCleanupUsers();
SUserObj *mnodeGetUser(char *name);
void *    mnodeGetNextUser(void *pIter, SUserObj **pUser);
void      mnodeCancelGetNextUser(void *pIter);
void      mnodeIncUserRef(SUserObj *pUser);
void      mnodeDecUserRef(SUserObj *pUser);
SUserObj *mnodeGetUserFromConn(void *pConn);
char *    mnodeGetUserFromMsg(void *pMnodeMsg);
int32_t   mnodeCreateUser(SAcctObj *pAcct, char *name, char *pass, void *pMsg);
void      mnodeDropAllUsers(SAcctObj *pAcct);

#ifdef __cplusplus
}
#endif

#endif
