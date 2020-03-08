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

#ifndef TDENGINE_MGMT_DNODE_INT_H
#define TDENGINE_MGMT_DNODE_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

extern void *mgmtStatusTimer;

void mgmtSendCreateTableMsg(SDCreateTableMsg *pCreate, SRpcIpSet *ipSet, void *ahandle);
void mgmtSendRemoveTableMsg(SDRemoveTableMsg *pRemove, SRpcIpSet *ipSet, void *ahandle);
void mgmtSendAlterStreamMsg(STableInfo *pTable, SRpcIpSet *ipSet, void *ahandle);
void mgmtSendCreateVnodeMsg(SVgObj *pVgroup, int32_t vnode, SRpcIpSet *ipSet, void *ahandle);
void mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle);
void mgmtSendOneFreeVnodeMsg(int32_t vnode, SRpcIpSet *ipSet, void *ahandle);
void mgmtSendRemoveVgroupMsg(SVgObj *pVgroup, void *ahandle);

int32_t mgmtInitDnodeInt();
void    mgmtCleanUpDnodeInt();

void mgmtSendMsgToDnode(SRpcIpSet *ipSet, int8_t msgType, void *pCont, int32_t contLen, void *ahandle);
void mgmtSendRspToDnode(void *pConn, int8_t msgType, int32_t code, void *pCont, int32_t contLen);
void mgmtProcessMsgFromDnode(char msgType, void *pCont, int32_t contLen, void *pConn, int32_t code);

#ifdef __cplusplus
}
#endif

#endif
