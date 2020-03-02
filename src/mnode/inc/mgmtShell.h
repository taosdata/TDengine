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

#ifndef TDENGINE_MGMT_SHELL_H
#define TDENGINE_MGMT_SHELL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

int32_t  mgmtInitShell();
void mgmtCleanUpShell();

extern int32_t (*mgmtCheckRedirectMsg)(void *pConn);
extern void (*mgmtProcessAlterAcctMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessCreateDnodeMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessCfgMnodeMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessDropMnodeMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessDropDnodeMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessDropAcctMsg)(void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtProcessCreateAcctMsg)(void *pCont, int32_t contLen, void *ahandle);

/*
 * If table not exist, will create it
 */
void mgmtProcessGetTableMeta(STableInfo *pTable, void *thandle);

/*
 * If vgroup not exist, will create vgroup
 */
void mgmtProcessCreateTable(SVgObj *pVgroup, SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta);

/*
 * If vgroup create returned, will then create table
 */
void mgmtProcessCreateVgroup(SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta);

#ifdef __cplusplus
}
#endif

#endif
