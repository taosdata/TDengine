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

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

int32_t mgmtInitVgroups();
void    mgmtCleanUpVgroups();
SVgObj *mgmtGetVgroup(int32_t vgId);
SVgObj *mgmtGetVgroupByVnode(uint32_t dnode, int32_t vnode);

SVgObj *mgmtCreateVgroup(SDbObj *pDb);
int32_t mgmtDropVgroup(SDbObj *pDb, SVgObj *pVgroup);
void    mgmtUpdateVgroup(SVgObj *pVgroup);

int32_t mgmtGetVgroupMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn);

void    mgmtSetVgroupIdPool();
SVgObj *mgmtGetAvailableVgroup(SDbObj *pDb);

void    mgmtAddTableIntoVgroup(SVgObj *pVgroup, STableInfo *pTable);
void    mgmtRemoveTableFromVgroup(SVgObj *pVgroup, STableInfo *pTable);

SVPeersMsg *mgmtBuildVpeersMsg(SVgObj *pVgroup, int32_t vnode);

SRpcIpSet mgmtGetIpSetFromVgroup(SVgObj *pVgroup);
SRpcIpSet mgmtGetIpSetFromIp(uint32_t ip);

#ifdef __cplusplus
}
#endif

#endif
