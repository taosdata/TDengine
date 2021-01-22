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

#ifndef TDENGINE_VNODE_READ_H
#define TDENGINE_VNODE_READ_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t vnodeInitRead(void);
void    vnodeCleanupRead(void);

int32_t vnodeWriteToRQueue(void *pVnode, void *pCont, int32_t contLen, int8_t qtype, void *rparam);
void    vnodeFreeFromRQueue(void *pVnode, SVReadMsg *pRead);
int32_t vnodeProcessRead(void *pVnode, SVReadMsg *pRead);
void    vnodeWaitReadCompleted(SVnodeObj *pVnode);

#ifdef __cplusplus
}
#endif

#endif
