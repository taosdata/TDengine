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

#ifndef TDENGINE_MNODE_VGROUP_H
#define TDENGINE_MNODE_VGROUP_H

#ifdef __cplusplus
extern "C" {
#endif

struct SMnodeMsg;

int32_t mnodeInitVgroups();
void    mnodeCleanupVgroups();
SVgObj *mnodeGetVgroup(int32_t vgId);
void    mnodeIncVgroupRef(SVgObj *pVgroup);
void    mnodeDecVgroupRef(SVgObj *pVgroup);
void    mnodeDropAllDbVgroups(SDbObj *pDropDb, bool sendMsg);
void    mnodeDropAllDnodeVgroups(SDnodeObj *pDropDnode);
void    mnodeUpdateAllDbVgroups(SDbObj *pAlterDb);

void *  mnodeGetNextVgroup(void *pIter, SVgObj **pVgroup);
void    mnodeUpdateVgroup(SVgObj *pVgroup);
void    mnodeUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *dnodeId, SVnodeLoad *pVload);

int32_t mnodeCreateVgroup(struct SMnodeMsg *pMsg, SDbObj *pDb);
void    mnodeDropVgroup(SVgObj *pVgroup, void *ahandle);
void    mnodeAlterVgroup(SVgObj *pVgroup, void *ahandle);
SVgObj *mnodeGetAvailableVgroup(SDbObj *pDb);

void    mnodeAddTableIntoVgroup(SVgObj *pVgroup, SChildTableObj *pTable);
void    mnodeRemoveTableFromVgroup(SVgObj *pVgroup, SChildTableObj *pTable);
void    mnodeSendCreateVnodeMsg(SVgObj *pVgroup, SRpcIpSet *ipSet, void *ahandle);
void    mnodeSendDropVnodeMsg(int32_t vgId, SRpcIpSet *ipSet, void *ahandle);
void    mnodeSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle);

SRpcIpSet mnodeGetIpSetFromVgroup(SVgObj *pVgroup);
SRpcIpSet mnodeGetIpSetFromIp(char *ep);

#ifdef __cplusplus
}
#endif

#endif
