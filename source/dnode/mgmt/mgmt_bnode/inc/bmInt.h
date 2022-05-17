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

#ifndef _TD_DND_BNODE_INT_H_
#define _TD_DND_BNODE_INT_H_

#include "dmUtil.h"

#include "bnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBnodeMgmt {
  SDnodeData   *pData;
  SBnode       *pBnode;
  SMsgCb        msgCb;
  const char   *path;
  const char   *name;
  SMultiWorker  writeWorker;
  SSingleWorker monitorWorker;
} SBnodeMgmt;

// bmHandle.c
SArray *bmGetMsgHandles();
int32_t bmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pReq);
int32_t bmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pReq);
int32_t bmProcessGetMonBmInfoReq(SBnodeMgmt *pMgmt, SRpcMsg *pReq);

// bmWorker.c
int32_t bmStartWorker(SBnodeMgmt *pMgmt);
void    bmStopWorker(SBnodeMgmt *pMgmt);
int32_t bmPutNodeMsgToWriteQueue(SBnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t bmPutNodeMsgToMonitorQueue(SBnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_BNODE_INT_H_*/