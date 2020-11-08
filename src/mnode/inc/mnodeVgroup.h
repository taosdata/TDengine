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
int64_t mnodeGetVgroupNum();
SVgObj *mnodeGetVgroup(int32_t vgId);
void    mnodeIncVgroupRef(SVgObj *pVgroup);
void    mnodeDecVgroupRef(SVgObj *pVgroup);
void    mnodeDropAllDbVgroups(SDbObj *pDropDb);
void    mnodeSendDropAllDbVgroupsMsg(SDbObj *pDropDb);
void    mnodeDropAllDnodeVgroups(SDnodeObj *pDropDnode);
//void  mnodeUpdateAllDbVgroups(SDbObj *pAlterDb);

void *  mnodeGetNextVgroup(void *pIter, SVgObj **pVgroup);
void    mnodeUpdateVgroup(SVgObj *pVgroup);
void    mnodeUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *pDnode, SVnodeLoad *pVload);
void    mnodeCheckUnCreatedVgroup(SDnodeObj *pDnode, SVnodeLoad *pVloads, int32_t openVnodes);

int32_t mnodeCreateVgroup(struct SMnodeMsg *pMsg);
void    mnodeDropVgroup(SVgObj *pVgroup, void *ahandle);
void    mnodeAlterVgroup(SVgObj *pVgroup, void *ahandle);
int32_t mnodeGetAvailableVgroup(struct SMnodeMsg *pMsg, SVgObj **pVgroup, int32_t *sid);

void    mnodeAddTableIntoVgroup(SVgObj *pVgroup, SCTableObj *pTable);
void    mnodeRemoveTableFromVgroup(SVgObj *pVgroup, SCTableObj *pTable);
void    mnodeSendDropVnodeMsg(int32_t vgId, SRpcEpSet *epSet, void *ahandle);
void    mnodeSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle);
void    mnodeSendAlterVgroupMsg(SVgObj *pVgroup);

SRpcEpSet mnodeGetEpSetFromVgroup(SVgObj *pVgroup);
SRpcEpSet mnodeGetEpSetFromIp(char *ep);

#ifdef __cplusplus
}
#endif

#endif
