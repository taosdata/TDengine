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

#ifndef _TD_DND_NODE_H_
#define _TD_DND_NODE_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dndOpenNode(SMgmtWrapper *pWrapper);
void    dndCloseNode(SMgmtWrapper *pWrapper);

// dndTransport.c
int32_t  dndInitTrans(SDnode *pDnode);
void     dndCleanupTrans(SDnode *pDnode);
SMsgCb   dndCreateMsgcb(SMgmtWrapper *pWrapper);
SProcCfg dndGenProcCfg(SMgmtWrapper *pWrapper);
int32_t  dndInitMsgHandle(SDnode *pDnode);
void     dndSendRecv(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);

// mgmt
void dmSetMgmtFp(SMgmtWrapper *pWrapper);
void bmSetMgmtFp(SMgmtWrapper *pWrapper);
void qmSetMgmtFp(SMgmtWrapper *pMgmt);
void smSetMgmtFp(SMgmtWrapper *pWrapper);
void vmSetMgmtFp(SMgmtWrapper *pWrapper);
void mmSetMgmtFp(SMgmtWrapper *pMgmt);

void dmGetMnodeEpSet(SDnodeData *pMgmt, SEpSet *pEpSet);
void dmUpdateMnodeEpSet(SDnodeData *pMgmt, SEpSet *pEpSet);
void dmSendRedirectRsp(SDnodeData *pMgmt, const SRpcMsg *pMsg);

void dmGetMonitorSysInfo(SMonSysInfo *pInfo);
void vmGetVnodeLoads(SMgmtWrapper *pWrapper, SMonVloadInfo *pInfo);
void mmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonMmInfo *mmInfo);
void vmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonVmInfo *vmInfo);
void qmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonQmInfo *qmInfo);
void smGetMonitorInfo(SMgmtWrapper *pWrapper, SMonSmInfo *smInfo);
void bmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonBmInfo *bmInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_NODE_H_*/