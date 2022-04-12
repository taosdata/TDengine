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

#ifndef _TD_DND_INT_H_
#define _TD_DND_INT_H_

#include "dndLog.h"
#include "dndDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// dndEnv.c
const char *dndStatStr(EDndRunStatus stat);
const char *dndNodeLogStr(EDndNodeType ntype);
const char *dndNodeProcStr(EDndNodeType ntype);
const char *dndEventStr(EDndEvent ev);

// dndExec.c
int32_t dndOpenNode(SMgmtWrapper *pWrapper);
void    dndCloseNode(SMgmtWrapper *pWrapper);

// dndFile.c
int32_t   dndReadFile(SMgmtWrapper *pWrapper, bool *pDeployed);
int32_t   dndWriteFile(SMgmtWrapper *pWrapper, bool deployed);
TdFilePtr dndCheckRunning(const char *dataDir);
int32_t   dndReadShmFile(SDnode *pDnode);
int32_t   dndWriteShmFile(SDnode *pDnode);

// dndInt.c
EDndRunStatus    dndGetStatus(SDnode *pDnode);
void          dndSetStatus(SDnode *pDnode, EDndRunStatus stat);
void          dndSetMsgHandle(SMgmtWrapper *pWrapper, tmsg_t msgType, NodeMsgFp nodeMsgFp, int8_t vgId);
SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dndMarkWrapper(SMgmtWrapper *pWrapper);
void          dndReleaseWrapper(SMgmtWrapper *pWrapper);
void          dndHandleEvent(SDnode *pDnode, EDndEvent event);
void          dndReportStartup(SDnode *pDnode, const char *pName, const char *pDesc);
void          dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

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

#endif /*_TD_DND_INT_H_*/