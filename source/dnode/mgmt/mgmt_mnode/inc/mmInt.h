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

#ifndef _TD_DND_MNODE_INT_H_
#define _TD_DND_MNODE_INT_H_

#include "dmUtil.h"
#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMnodeMgmt {
  SDnodeData   *pData;
  SMnode       *pMnode;
  SMsgCb        msgCb;
  const char   *path;
  const char   *name;
  SSingleWorker queryWorker;
  SSingleWorker readWorker;
  SSingleWorker writeWorker;
  SSingleWorker syncWorker;
  SSingleWorker monitorWorker;
  SReplica      replicas[TSDB_MAX_REPLICA];
  int8_t        replica;
  int8_t        selfIndex;
} SMnodeMgmt;

// mmFile.c
int32_t mmReadFile(SMnodeMgmt *pMgmt, bool *pDeployed);
int32_t mmWriteFile(SMnodeMgmt *pMgmt, SDCreateMnodeReq *pReq, bool deployed);

// mmInt.c
int32_t mmAlter(SMnodeMgmt *pMgmt, SDAlterMnodeReq *pReq);

// mmHandle.c
SArray *mmGetMsgHandles();
int32_t mmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t mmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t mmProcessAlterReq(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmProcessGetMonitorInfoReq(SMnodeMgmt *pMgmt, SRpcMsg *pReq);
int32_t mmProcessGetLoadsReq(SMnodeMgmt *pMgmt, SRpcMsg *pReq);

// mmWorker.c
int32_t mmStartWorker(SMnodeMgmt *pMgmt);
void    mmStopWorker(SMnodeMgmt *pMgmt);
int32_t mmPutNodeMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutNodeMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutNodeMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutNodeMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutNodeMsgToMonitorQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutRpcMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc);
int32_t mmPutRpcMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc);
int32_t mmPutRpcMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc);
int32_t mmPutRpcMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/
