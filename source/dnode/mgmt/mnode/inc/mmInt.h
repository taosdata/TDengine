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

#include "mm.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMnodeMgmt {
  int32_t       refCount;
  int8_t        deployed;
  int8_t        dropped;
  int8_t        replica;
  int8_t        selfIndex;
  SReplica      replicas[TSDB_MAX_REPLICA];
  SMnode       *pMnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SRWLatch      latch;
  SDnodeWorker  readWorker;
  SDnodeWorker  writeWorker;
  SDnodeWorker  syncWorker;
} SMnodeMgmt;

// mmFile.c
int32_t mmReadFile(SMnodeMgmt *pMgmt);
int32_t mmWriteFile(SMnodeMgmt *pMgmt);

// mmInt.c
SMnode *mmAcquire(SMnodeMgmt *pMgmt);
void    mmRelease(SMnodeMgmt *pMgmt, SMnode *pMnode);
int32_t mmOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption);
int32_t mmAlter(SMnodeMgmt *pMgmt, SMnodeOpt *pOption);
int32_t mmDrop(SMnodeMgmt *pMgmt);
int32_t mmBuildOptionFromReq(SMnodeMgmt *pMgmt, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate);

// mmMsg.c
int32_t mmProcessAlterReq(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);

// mmWorker.c
int32_t mmStartWorker(SMnodeMgmt *pMgmt);
void    mmStopWorker(SMnodeMgmt *pMgmt);
int32_t mmProcessWriteMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t mmProcessSyncMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t mmProcessReadMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);

int32_t mmPutMsgToWriteQueue(void *wrapper, SRpcMsg *pRpcMsg);
int32_t mmPutMsgToReadQueue(void *wrapper, SRpcMsg *pRpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/