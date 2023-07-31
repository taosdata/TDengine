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
  SDnodeData    *pData;
  SMnode        *pMnode;
  SMsgCb         msgCb;
  const char    *path;
  const char    *name;
  SSingleWorker  queryWorker;
  SSingleWorker  fetchWorker;
  SSingleWorker  readWorker;
  SSingleWorker  writeWorker;
  SSingleWorker  syncWorker;
  SSingleWorker  syncRdWorker;
  bool           stopped;
  int32_t        refCount;
  TdThreadRwlock lock;
} SMnodeMgmt;

// mmFile.c
int32_t mmReadFile(const char *path, SMnodeOpt *pOption);
int32_t mmWriteFile(const char *path, const SMnodeOpt *pOption);

// mmHandle.c
SArray *mmGetMsgHandles();
int32_t mmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t mmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);

// mmWorker.c
int32_t mmStartWorker(SMnodeMgmt *pMgmt);
void    mmStopWorker(SMnodeMgmt *pMgmt);
int32_t mmPutMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToSyncRdQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToFetchQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t mmPutMsgToQueue(SMnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/
