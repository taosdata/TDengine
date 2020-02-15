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

#ifndef TDENGINE_VNODE_PEER_H
#define TDENGINE_VNODEPEER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "taosdef.h"

/*
 * Initialize the resources
 */
int32_t vnodeInitPeers(int numOfThreads);

/*
 * Free the resources
 */
void vnodeCleanUpPeers();

/*
 * Start a vnode synchronization process
 */
int32_t vnodeOpenPeer(int32_t vnode);

/*
 * Update the peerinfo of vnode
 */
int32_t vnodeConfigPeer(SVpeerDescArray msg);

/*
 * Close a vnode synchronization process
 */
void vnodeCleanUpPeer(int32_t vnode);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEPEER_H
