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

#include "tsdb.h"
#include "taosmsg.h"

/*
 * Open all Vnodes in the local data directory
 */
int32_t dnodeOpenVnodes();

/*
 * Close all Vnodes that have been created and opened
 */
int32_t dnodeCloseVnodes();

/*
 * Check if vnode already exists
 */
int32_t dnodeCheckVnodeExist(int vid);

/*
 * Create vnode with specified configuration and open it
 */
tsdb_repo_t* dnodeCreateVnode(int vid, SVnodeCfg *cfg);

/*
 * Modify vnode configuration information
 */
int32_t dnodeConfigVnode(int vid, SVnodeCfg *cfg);

/*
 * Modify vnode replication information
 */
int32_t dnodeConfigVnodePeers(int vid, SVpeerCfg *cfg);

/*
 * Remove vnode from local repository
 */
int32_t dnodeDropVnode(int vid);

/*
 * Get the vnode object that has been opened
 */
tsdb_repo_t* dnodeGetVnode(int vid);

#ifdef __cplusplus
}
#endif

#endif
