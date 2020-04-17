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

typedef enum {
  TAOS_DN_STATUS_OFFLINE,
  TAOS_DN_STATUS_DROPPING,
  TAOS_DN_STATUS_BALANCING,
  TAOS_DN_STATUS_READY
} EDnodeStatus;

int32_t mgmtInitDnodes();
void    mgmtCleanupDnodes();

char*   mgmtGetDnodeStatusStr(int32_t dnodeStatus);
void    mgmtMonitorDnodeModule();

int32_t mgmtGetDnodesNum();
void *  mgmtGetNextDnode(void *pNode, SDnodeObj **pDnode);
void    mgmtReleaseDnode(SDnodeObj *pDnode);
void *  mgmtGetDnode(int32_t dnodeId);
void *  mgmtGetDnodeByIp(uint32_t ip);
void    mgmtUpdateDnode(SDnodeObj *pDnode);
int32_t mgmtDropDnode(SDnodeObj *pDnode);

#ifdef __cplusplus
}
#endif

#endif
