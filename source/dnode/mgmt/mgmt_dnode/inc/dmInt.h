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

#ifndef _TD_DND_QNODE_INT_H_
#define _TD_DND_QNODE_INT_H_

#include "dmUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDnodeMgmt {
  SDnodeData         *pData;
  SMsgCb              msgCb;
  const char         *path;
  const char         *name;
  TdThread            statusThread;
  TdThread            monitorThread;
  SSingleWorker       mgmtWorker;
  ProcessCreateNodeFp processCreateNodeFp;
  ProcessDropNodeFp   processDropNodeFp;
  IsNodeRequiredFp    isNodeRequiredFp;
} SDnodeMgmt;

// dmHandle.c
SArray *dmGetMsgHandles();
void    dmSendStatusReq(SDnodeMgmt *pMgmt);
int32_t dmProcessConfigReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessAuthRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessGrantRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessServerRunStatus(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);

// dmMonitor.c
void dmGetVnodeLoads(SDnodeMgmt *pMgmt, SMonVloadInfo *pInfo);
void dmGetMnodeLoads(SDnodeMgmt *pMgmt, SMonMloadInfo *pInfo);
void dmSendMonitorReport(SDnodeMgmt *pMgmt);

// dmWorker.c
int32_t dmPutNodeMsgToMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmStartStatusThread(SDnodeMgmt *pMgmt);
void    dmStopStatusThread(SDnodeMgmt *pMgmt);
int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt);
void    dmStopMonitorThread(SDnodeMgmt *pMgmt);
int32_t dmStartWorker(SDnodeMgmt *pMgmt);
void    dmStopWorker(SDnodeMgmt *pMgmt);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_QNODE_INT_H_*/