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

#ifndef TDENGINE_VNODE_MAIN_H
#define TDENGINE_VNODE_MAIN_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vgId);
int32_t vnodeAlter(void *pVnode, SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeSync(int32_t vgId);
int32_t vnodeClose(int32_t vgId);
void    vnodeCleanUp(SVnodeObj *pVnode);
void    vnodeDestroy(SVnodeObj *pVnode);

#ifdef __cplusplus
}
#endif

#endif
