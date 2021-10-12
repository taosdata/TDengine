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

#ifndef _TD_VNODE_READ_H_
#define _TD_VNODE_READ_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t    vnodeInitRead();
void       vnodeCleanupRead();
taos_queue vnodeAllocQueryQueue(SVnode *pVnode);
taos_queue vnodeAllocFetchQueue(SVnode *pVnode);
void       vnodeFreeQueryQueue(taos_queue pQueue);
void       vnodeFreeFetchQueue(taos_queue pQueue);

void vnodeProcessReadMsg(SRpcMsg *pRpcMsg);
int32_t vnodeReputPutToRQueue(SVnode *pVnode, void **qhandle, void *ahandle);

void vnodeStartRead(SVnode *pVnode);
void vnodeStopRead(SVnode *pVnode);
void vnodeWaitReadCompleted(SVnode *pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_READ_H_*/
