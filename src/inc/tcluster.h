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

#ifndef TDENGINE_CLUSTER_H
#define TDENGINE_CLUSTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

struct _dnode_obj;

enum _TAOS_DN_STATUS {
  TAOS_DN_STATUS_OFFLINE,
  TAOS_DN_STATUS_DROPPING,
  TAOS_DN_STATUS_BALANCING,
  TAOS_DN_STATUS_READY
};

int32_t clusterInit();
void    clusterCleanUp();
char*   clusterGetDnodeStatusStr(int32_t dnodeStatus);
bool    clusterCheckModuleInDnode(struct _dnode_obj *pDnode, int moduleType);
void    clusterMonitorDnodeModule();

int32_t clusterInitDnodes();
void    clusterCleanupDnodes();
int32_t clusterGetDnodesNum();
void *  clusterGetNextDnode(void *pNode, struct _dnode_obj **pDnode);
void    clusterReleaseDnode(struct _dnode_obj *pDnode);
void *  clusterGetDnode(int32_t dnodeId);
void *  clusterGetDnodeByIp(uint32_t ip);
void    clusterUpdateDnode(struct _dnode_obj *pDnode);
int32_t clusterDropDnode(struct _dnode_obj *pDnode);

#ifdef __cplusplus
}
#endif

#endif
