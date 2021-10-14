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

#ifndef _TD_VNODE_MAIN_H_
#define _TD_VNODE_MAIN_H_

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t vnodeInitMain();
void    vnodeCleanupMain();

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vgId);
int32_t vnodeAlter(SVnode *pVnode, SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeSync(int32_t vgId);
int32_t vnodeClose(int32_t vgId);
void    vnodeCleanUp(SVnode *pVnode);
void    vnodeDestroy(SVnode *pVnode);
int32_t vnodeCompact(int32_t vgId);
void    vnodeBackup(int32_t vgId);
void    vnodeGetStatus(struct SStatusMsg *status);

SVnode *vnodeAcquire(int32_t vgId);
SVnode *vnodeAcquireNotClose(int32_t vgId);
void    vnodeRelease(SVnode *pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_MAIN_H_*/
