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

#ifndef _TD_DND_VNODES_H_
#define _TD_DND_VNODES_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dndInt.h"

SMgmtFp vmGetMgmtFp() ;


int32_t dndInitVnodes(SDnode *pDnode);
void    dndCleanupVnodes(SDnode *pDnode);
void    dndGetVnodeLoads(SDnode *pDnode, SArray *pLoads);
void    dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);

int32_t dndProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *pReq);

int32_t dndPutReqToVQueryQ(SDnode *pDnode, SRpcMsg *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_H_*/