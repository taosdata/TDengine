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

#ifndef _TD_DND_SNODE_INT_H_
#define _TD_DND_SNODE_INT_H_

#include "dmUtil.h"

#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSnodeMgmt {
  SDnodeData   *pData;
  SSnode       *pSnode;
  SMsgCb        msgCb;
  const char   *path;
  const char   *name;
  int8_t        writeWorkerInUse;
  SArray       *writeWroker;  // SArray<SMultiWorker*>
  SSingleWorker streamWorker;
} SSnodeMgmt;

// smHandle.c
SArray *smGetMsgHandles();
int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);

// smWorker.c
int32_t smStartWorker(SSnodeMgmt *pMgmt);
void    smStopWorker(SSnodeMgmt *pMgmt);
int32_t smPutMsgToQueue(SSnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pMsg);
int32_t smPutNodeMsgToMgmtQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t smPutNodeMsgToWriteQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t smPutNodeMsgToStreamQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg);
void    sndEnqueueStreamDispatch(SSnode *pSnode, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/
