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

#ifndef TDENGINE_VNODE_CANCEL_H
#define TDENGINE_VNODE_CANCEL_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnode.h"
#include "vnodeInt.h"

int32_t vnodeInitCWorker();
void    vnodeCleanupCWorker();
int32_t vnodeWriteIntoCQueue(SVnodeObj *pVnode, SVReadMsg *pRead);

#ifdef __cplusplus
}
#endif

#endif
