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
#include "mnode.h"

int32_t mgmtInitShell();
void mgmtCleanUpShell();
void mgmtAddShellMsgHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg));

typedef int32_t (*SShowMetaFp)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
void mgmtAddShellShowMetaHandle(uint8_t showType, SShowMetaFp fp);
void mgmtAddShellShowRetrieveHandle(uint8_t showType, SShowRetrieveFp fp);

//extern int32_t (*mgmtCheckRedirect)(void *pConn);
//
///*
// * If table not exist, will create it
// */
//void mgmtProcessGetTableMeta(STableInfo *pTable, void *thandle);
//
///*
// * If vgroup not exist, will create vgroup
// */
//void mgmtProcessCreateTable(SVgObj *pVgroup, SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta);
//
///*
// * If vgroup create returned, will then create table
// */
//void mgmtProcessCreateVgroup(SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta);

#ifdef __cplusplus
}
#endif

#endif
