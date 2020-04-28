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

#ifndef TDENGINE_MGMT_VGROUP_H
#define TDENGINE_MGMT_VGROUP_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mgmtDef.h"

enum _TSDB_VG_STATUS {
  TSDB_VG_STATUS_READY,
  TSDB_VG_STATUS_UPDATE
};

int32_t mgmtInitVgroups();
void    mgmtCleanUpVgroups();
SVgObj *mgmtGetVgroup(int32_t vgId);
void    mgmtIncVgroupRef(SVgObj *pVgroup);
void    mgmtDecVgroupRef(SVgObj *pVgroup);
void    mgmtDropAllVgroups(SDbObj *pDropDb);

void *  mgmtGetNextVgroup(void *pNode, SVgObj **pVgroup);
void    mgmtUpdateVgroup(SVgObj *pVgroup);
void    mgmtUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *dnodeId, SVnodeLoad *pVload);

void    mgmtCreateVgroup(SQueuedMsg *pMsg, SDbObj *pDb);
void    mgmtDropVgroup(SVgObj *pVgroup, void *ahandle);
void    mgmtAlterVgroup(SVgObj *pVgroup, void *ahandle);
SVgObj *mgmtGetAvailableVgroup(SDbObj *pDb);

void    mgmtAddTableIntoVgroup(SVgObj *pVgroup, SChildTableObj *pTable);
void    mgmtRemoveTableFromVgroup(SVgObj *pVgroup, SChildTableObj *pTable);
void    mgmtSendCreateVnodeMsg(SVgObj *pVgroup, SRpcIpSet *ipSet, void *ahandle);
void    mgmtSendDropVnodeMsg(int32_t vgId, SRpcIpSet *ipSet, void *ahandle);
void    mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle);

SRpcIpSet mgmtGetIpSetFromVgroup(SVgObj *pVgroup);
SRpcIpSet mgmtGetIpSetFromIp(char *ep);

#ifdef __cplusplus
}
#endif

#endif
