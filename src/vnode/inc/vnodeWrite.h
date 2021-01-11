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

#ifndef TDENGINE_VNODE_WRITE_H
#define TDENGINE_VNODE_WRITE_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t vnodeInitWrite(void);
void    vnodeCleanupWrite(void);

int32_t vnodeWriteToWQueue(void *pVnode, void *pHead, int32_t qtype, void *pRpcMsg);
void    vnodeFreeFromWQueue(void *pVnode, SVWriteMsg *pWrite);
int32_t vnodeProcessWrite(void *pVnode, void *pHead, int32_t qtype, void *pRspRet);
void    vnodeWaitWriteCompleted(SVnodeObj *pVnode);

#ifdef __cplusplus
}
#endif

#endif