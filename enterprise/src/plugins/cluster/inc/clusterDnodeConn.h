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

#ifndef TDENGINE_CLUSTER_DNODE_CONN_H
#define TDENGINE_CLUSTER_DNODE_CONN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

uint32_t dnodeGetMgmtIp();

int32_t dnodeInitMgmtImp();
void dnodeCleanUpMgmtImp();
void dnodeInitMgmtIpImp();

void dnodeProcessStatusRspImp(void *pCont, int32_t contLen, int8_t msgType, void *pConn);
void dnodeSendMsgToMnodeImp(int8_t msgType, void *pCont, int32_t contLen);
void dnodeSendRspToMnodeImp(void *handle, int32_t code, void *pCont, int contLen);

#ifdef __cplusplus
}
#endif

#endif
