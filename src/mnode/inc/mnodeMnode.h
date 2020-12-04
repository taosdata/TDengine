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

#ifndef TDENGINE_MNODE_MNODE_H
#define TDENGINE_MNODE_MNODE_H

#ifdef __cplusplus
extern "C" {
#endif

struct SMnodeObj;

typedef enum {
  TAOS_MN_STATUS_OFFLINE,
  TAOS_MN_STATUS_DROPPING,
  TAOS_MN_STATUS_READY
} EMnodeStatus;

int32_t mnodeInitMnodes();
void    mnodeCleanupMnodes();

void    mnodeCreateMnode(int32_t dnodeId, char *dnodeEp, bool needConfirm);
int32_t mnodeDropMnode(int32_t dnodeId);
void    mnodeDropMnodeLocal(int32_t dnodeId);

void *  mnodeGetMnode(int32_t mnodeId);
int32_t mnodeGetMnodesNum();
void *  mnodeGetNextMnode(void *pIter, struct SMnodeObj **pMnode);
void    mnodeCancelGetNextMnode(void *pIter);
void    mnodeIncMnodeRef(struct SMnodeObj *pMnode);
void    mnodeDecMnodeRef(struct SMnodeObj *pMnode);

char *  mnodeGetMnodeRoleStr();
void    mnodeGetMnodeEpSetForPeer(SRpcEpSet *epSet);
void    mnodeGetMnodeEpSetForShell(SRpcEpSet *epSet);
char*   mnodeGetMnodeMasterEp();

void    mnodeGetMnodeInfos(void *mnodes);
void    mnodeUpdateMnodeEpSet(SMInfos *pMnodes);

#ifdef __cplusplus
}
#endif

#endif
