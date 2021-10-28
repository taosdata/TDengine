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

SVnode *vnodeAcquireInAllState(int32_t vgId);
SVnode *vnodeAcquire(int32_t vgId);
void    vnodeRelease(SVnode *pVnode);

int32_t vnodeCreateVnode(int32_t vgId, SVnodeCfg *pCfg);
int32_t vnodeAlterVnode(SVnode *pVnode, SVnodeCfg *pCfg);
int32_t vnodeDropVnode(SVnode *pVnode);
int32_t vnodeSyncVnode(SVnode *pVnode);
int32_t vnodeCompactVnode(SVnode *pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_MAIN_H_*/
