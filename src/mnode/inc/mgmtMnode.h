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

#ifndef TDENGINE_MGMT_MNODE_H
#define TDENGINE_MGMT_MNODE_H

#ifdef __cplusplus
extern "C" {
#endif

struct SMnodeObj;

typedef enum {
  TAOS_MN_STATUS_OFFLINE,
  TAOS_MN_STATUS_DROPPING,
  TAOS_MN_STATUS_READY
} EMnodeStatus;

int32_t mgmtInitMnodes();
void    mgmtCleanupMnodes();

int32_t mgmtAddMnode(int32_t dnodeId);
int32_t mgmtDropMnode(int32_t dnodeId);

void *  mgmtGetMnode(int32_t mnodeId);
int32_t mgmtGetMnodesNum();
void *  mgmtGetNextMnode(void *pNode, struct SMnodeObj **pMnode);
void    mgmtReleaseMnode(struct SMnodeObj *pMnode);

char *  mgmtGetMnodeRoleStr();
void    mgmtGetMnodeIpList(SRpcIpSet *ipSet, bool usePublicIp);
void    mgmtGetMnodeList(void *mnodes);

#ifdef __cplusplus
}
#endif

#endif
