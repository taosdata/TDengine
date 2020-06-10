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

#ifndef TDENGINE_MNODE_DNODE_H
#define TDENGINE_MNODE_DNODE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TAOS_DN_STATUS_OFFLINE,
  TAOS_DN_STATUS_DROPPING,
  TAOS_DN_STATUS_BALANCING,
  TAOS_DN_STATUS_READY
} EDnodeStatus;

typedef enum {
  TAOS_DN_ALTERNATIVE_ROLE_ANY,
  TAOS_DN_ALTERNATIVE_ROLE_MNODE,
  TAOS_DN_ALTERNATIVE_ROLE_VNODE
} EDnodeAlternativeRole;

int32_t mnodeInitDnodes();
void    mnodeCleanupDnodes();

char*   mnodeGetDnodeStatusStr(int32_t dnodeStatus);
void    mgmtMonitorDnodeModule();

int32_t mnodeGetDnodesNum();
int32_t mnodeGetOnlinDnodesNum();
void *  mnodeGetNextDnode(void *pIter, SDnodeObj **pDnode);
void    mnodeIncDnodeRef(SDnodeObj *pDnode);
void    mnodeDecDnodeRef(SDnodeObj *pDnode);
void *  mnodeGetDnode(int32_t dnodeId);
void *  mnodeGetDnodeByEp(char *ep);
void    mnodeUpdateDnode(SDnodeObj *pDnode);
int32_t mnodeDropDnode(SDnodeObj *pDnode);

extern int32_t tsAccessSquence;

#ifdef __cplusplus
}
#endif

#endif
