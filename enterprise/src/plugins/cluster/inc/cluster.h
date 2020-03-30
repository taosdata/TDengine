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

#ifndef TDENGINE_MGMT_DNODE_H
#define TDENGINE_MGMT_DNODE_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnode.h"

int32_t clusterInit();
void    clusterCleanUp();
int32_t clusterGetDnodesNum();
SDnodeObj* clusterGetDnode(int32_t dnodeId);
SDnodeObj* clusterGetDnodeByIp(uint32_t ip);


// bool mgmtCheckDnodeInRemoveState(SDnodeObj *pDnode);
// bool mgmtCheckDnodeInOfflineState(SDnodeObj *pDnode);
// bool mgmtCheckModuleInDnode(SDnodeObj *pDnode, int32_t moduleType);
// void mgmtSetDnodeUnRemove(SDnodeObj *pDnode);
// void mgmtSetDnodeMaxVnodes(SDnodeObj *pDnode);
// void mgmtCalcNumOfFreeVnodes(SDnodeObj *pDnode);
// void mgmtSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes, int32_t vgId);
// void mgmtUnSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes);


#ifdef __cplusplus
}
#endif

#endif
