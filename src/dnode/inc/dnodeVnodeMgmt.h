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

#ifndef TDENGINE_DNODE_VNODE_MGMT_H
#define TDENGINE_DNODE_VNODE_MGMT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "taosdef.h"
#include "taosmsg.h"
#include "tstatus.h"

/*
 * Open all Vnodes in the local data directory
 */
int32_t dnodeOpenVnodes();

/*
 * Close all Vnodes that have been created and opened
 */
int32_t dnodeCleanupVnodes();

/*
 * Check if vnode already exists
 */
bool dnodeCheckVnodeExist(int32_t vid);

/*
 * Create vnode with specified configuration and open it
 * if exist, config it
 */
int32_t dnodeCreateVnode(int32_t vnode, SVPeersMsg *cfg);

/*
 * Remove vnode from local repository
 */
int32_t dnodeDropVnode(int32_t vnode);

/*
 * Get the vnode object that has been opened
 */
//tsdb_repo_t* dnodeGetVnode(int vid);
void* dnodeGetVnode(int vid);

/*
 * get the status of vnode
 */
EVnodeStatus dnodeGetVnodeStatus(int32_t vnode);

/*
 * Check if vnode already exists, and table exist in this vnode
 */
bool dnodeCheckTableExist(int32_t vnode, int32_t sid, int64_t uid);

#ifdef __cplusplus
}
#endif

#endif
