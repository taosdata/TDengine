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

#ifndef TDENGINE_VNODE_MGMT_H
#define TDENGINE_VNODE_MGMT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t vnodeInitMgmt();
void    vnodeCleanupMgmt();

void*   vnodeAcquire(int32_t vgId);
void    vnodeRelease(void *pVnode);
void*   vnodeGetWal(void *pVnode);

int32_t vnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes);
void    vnodeBuildStatusMsg(void *pStatus);
void    vnodeSetAccess(SVgroupAccess *pAccess, int32_t numOfVnodes);

void    vnodeAddIntoHash(SVnodeObj* pVnode);
void    vnodeRemoveFromHash(SVnodeObj * pVnode);

#ifdef __cplusplus
}
#endif

#endif
