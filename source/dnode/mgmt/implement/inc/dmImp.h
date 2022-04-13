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

#ifndef _TD_DND_IMP_H_
#define _TD_DND_IMP_H_

#include "dmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dmOpenNode(SMgmtWrapper *pWrapper);
void    dmCloseNode(SMgmtWrapper *pWrapper);

// dmTransport.c
int32_t  dmInitTrans(SDnode *pDnode);
void     dmCleanupTrans(SDnode *pDnode);
SProcCfg dmGenProcCfg(SMgmtWrapper *pWrapper);
int32_t  dmInitMsgHandle(SDnode *pDnode);
void     dmSendRecv(SDnode *pDnode, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
void     dmSendToMnodeRecv(SDnode *pDnode, SRpcMsg *pReq, SRpcMsg *pRsp);

// dmEps.c
int32_t dmReadEps(SDnode *pDnode);
int32_t dmWriteEps(SDnode *pDnode);
void    dmUpdateEps(SDnode *pDnode, SArray *pDnodeEps);

// dmHandle.c
void    dmSendStatusReq(SDnode *pDnode);
int32_t dmProcessConfigReq(SDnode *pDnode, SNodeMsg *pMsg);
int32_t dmProcessAuthRsp(SDnode *pDnode, SNodeMsg *pMsg);
int32_t dmProcessGrantRsp(SDnode *pDnode, SNodeMsg *pMsg);
int32_t dmProcessCreateNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg);
int32_t dmProcessDropNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg);

// dmMonitor.c
void dmGetVnodeLoads(SDnode *pDnode, SMonVloadInfo *pInfo);
void dmSendMonitorReport(SDnode *pDnode);

// dmWorker.c
int32_t dmStartStatusThread(SDnode *pDnode);
void    dmStopStatusThread(SDnode *pDnode);
int32_t dmStartMonitorThread(SDnode *pDnode);
void    dmStopMonitorThread(SDnode *pDnode);
int32_t dmStartWorker(SDnode *pDnode);
void    dmStopWorker(SDnode *pDnode);
int32_t dmProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t dmProcessStatusMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

// mgmt nodes
void dmSetMgmtFp(SMgmtWrapper *pWrapper);
void bmSetMgmtFp(SMgmtWrapper *pWrapper);
void qmSetMgmtFp(SMgmtWrapper *pWrapper);
void smSetMgmtFp(SMgmtWrapper *pWrapper);
void vmSetMgmtFp(SMgmtWrapper *pWrapper);
void mmSetMgmtFp(SMgmtWrapper *pWrapper);

void vmGetVnodeLoads(SMgmtWrapper *pWrapper, SMonVloadInfo *pInfo);
void mmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonMmInfo *mmInfo);
void vmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonVmInfo *vmInfo);
void qmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonQmInfo *qmInfo);
void smGetMonitorInfo(SMgmtWrapper *pWrapper, SMonSmInfo *smInfo);
void bmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonBmInfo *bmInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_IMP_H_*/