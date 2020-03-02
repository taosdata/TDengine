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

#ifndef TDENGINE_MODULE_CLUSTER_H
#define TDENGINE_MODULE_CLUSTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "mnode.h"

void clusterInit();

// dnodeSystem
extern int32_t (*dnodeInitMgmtFp)();
extern void (*dnodeInitMgmtIpFp)();

// dnodeMgmt
extern void (*dnodeSendMsgToMnodeFp)(int8_t msgType, void *pCont, int32_t contLen, void *ahandle);
extern void (*dnodeSendRspToMnodeFp)(void *handle, int32_t code, void *pCont, int contLen);
extern void (*dnodeProcessStatusRspFp)(int8_t *pCont, int32_t contLen, int8_t msgType, void *pConn);

// mgmtDnodeInt
extern int32_t (*mgmtInitDnodeIntFp)();
extern void    (*mgmtCleanUpDnodeIntFp);

extern void (*mgmtSendMsgToDnodeFp)(SRpcIpSet *ipSet, int8_t msgType, void *pCont, int32_t contLen, void *ahandle);
extern void (*mgmtSendRspToDnodeFp)(void *handle, int32_t code, void *pCont, int contLen);

// mgmtDnode & clusterDnode
extern int32_t    (*mgmtInitDnodesFp)();
extern void       (*mgmtCleanUpDnodesFp)();
extern SDnodeObj *(*mgmtGetDnodeFp)(uint32_t ip);
extern int32_t    (*mgmtGetDnodesNumFp)();
extern void *     (*mgmtGetNextDnodeFp)(SShowObj *pShow, SDnodeObj **pDnode);



#ifdef __cplusplus
}
#endif

#endif
