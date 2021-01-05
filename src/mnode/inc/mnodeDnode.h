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

typedef enum EDnodeOfflineReason {
  TAOS_DN_OFF_ONLINE = 0,
  TAOS_DN_OFF_STATUS_MSG_TIMEOUT,
  TAOS_DN_OFF_STATUS_NOT_RECEIVED,
  TAOS_DN_OFF_RESET_BY_MNODE,
  TAOS_DN_OFF_VERSION_NOT_MATCH,
  TAOS_DN_OFF_DNODE_ID_NOT_MATCH,
  TAOS_DN_OFF_CLUSTER_ID_NOT_MATCH,
  TAOS_DN_OFF_NUM_OF_MNODES_NOT_MATCH,
  TAOS_DN_OFF_ENABLE_BALANCE_NOT_MATCH,
  TAOS_DN_OFF_MN_EQUAL_VN_NOT_MATCH,
  TAOS_DN_OFF_OFFLINE_THRESHOLD_NOT_MATCH,
  TAOS_DN_OFF_STATUS_INTERVAL_NOT_MATCH,
  TAOS_DN_OFF_MAX_TAB_PER_VN_NOT_MATCH,
  TAOS_DN_OFF_MAX_VG_PER_DB_NOT_MATCH,
  TAOS_DN_OFF_ARBITRATOR_NOT_MATCH,
  TAOS_DN_OFF_TIME_ZONE_NOT_MATCH,
  TAOS_DN_OFF_LOCALE_NOT_MATCH,
  TAOS_DN_OFF_CHARSET_NOT_MATCH,
  TAOS_DN_OFF_FLOW_CTRL_NOT_MATCH,
  TAOS_DN_OFF_SLAVE_QUERY_NOT_MATCH,
  TAOS_DN_OFF_ADJUST_MASTER_NOT_MATCH,
  TAOS_DN_OFF_OTHERS
} EDnodeOfflineReason;

extern char* dnodeStatus[];
extern char* dnodeRoles[];

int32_t mnodeInitDnodes();
void    mnodeCleanupDnodes();

int32_t mnodeGetDnodesNum();
int32_t mnodeGetOnlinDnodesCpuCoreNum();
int32_t mnodeGetOnlineDnodesNum();
void *  mnodeGetNextDnode(void *pIter, SDnodeObj **pDnode);
void    mnodeCancelGetNextDnode(void *pIter);
void    mnodeIncDnodeRef(SDnodeObj *pDnode);
void    mnodeDecDnodeRef(SDnodeObj *pDnode);
void *  mnodeGetDnode(int32_t dnodeId);
void *  mnodeGetDnodeByEp(char *ep);
void    mnodeUpdateDnode(SDnodeObj *pDnode);
int32_t mnodeDropDnode(SDnodeObj *pDnode, void *pMsg);

extern int32_t tsAccessSquence;

#ifdef __cplusplus
}
#endif

#endif
