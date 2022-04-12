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

#ifndef _TD_DND_DNODE_INT_H_
#define _TD_DND_DNODE_INT_H_

#include "dndNode.h"

#ifdef __cplusplus
extern "C" {
#endif



// dmFile.c
int32_t dmReadFile(SDnodeData *pMgmt);
int32_t dmWriteFile(SDnodeData *pMgmt);
void    dmUpdateDnodeEps(SDnodeData *pMgmt, SArray *pDnodeEps);

// dmHandle.c
void    dmInitMsgHandle(SMgmtWrapper *pWrapper);
void    dmSendStatusReq(SDnodeData *pMgmt);
int32_t dmProcessConfigReq(SDnodeData *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessStatusRsp(SDnodeData *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessAuthRsp(SDnodeData *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessGrantRsp(SDnodeData *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessCDnodeReq(SDnode *pDnode, SNodeMsg *pMsg);

// dmMonitor.c
void dmGetVnodeLoads(SMgmtWrapper *pWrapper, SMonVloadInfo *pInfo);
void dmSendMonitorReport(SDnode *pDnode);

// dmWorker.c
int32_t dmStartThread(SDnodeData *pMgmt);
int32_t dmStartWorker(SDnodeData *pMgmt);
void    dmStopWorker(SDnodeData *pMgmt);
int32_t dmProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t dmProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_INT_H_*/