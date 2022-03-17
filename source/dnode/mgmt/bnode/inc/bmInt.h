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

#include "mm.h"
#include "dm.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBnodeMgmt {
  int32_t       refCount;
  int8_t        deployed;
  int8_t        dropped;
  SBnode       *pBnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SRWLatch      latch;
  SDnodeWorker  writeWorker;
} SBnodeMgmt;

// mmFile.c
int32_t bmReadFile(SBnodeMgmt *pMgmt);
int32_t bmWriteFile(SBnodeMgmt *pMgmt);

SBnode *bmAcquire(SBnodeMgmt *pMgmt);
void    bmRelease(SBnodeMgmt *pMgmt, SBnode *pBnode);

// SBnode *mmAcquire(SMnodeMgmt *pMgmt);
// void    mmRelease(SMnodeMgmt *pMgmt, SBnode *pMnode);
// int32_t mmOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption);
// int32_t mmAlter(SMnodeMgmt *pMgmt, SMnodeOpt *pOption);
// int32_t mmDrop(SMnodeMgmt *pMgmt);


// void bmGetMgmtFp(SMgmtWrapper *pMgmt);

// int32_t dndInitBnode(SDnode *pDnode);
// void    dndCleanupBnode(SDnode *pDnode);

// void    dndProcessBnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
// int32_t bmProcessCreateReq(SBnodeMgmt *pMgmt, SNodeMsg *pRpcMsg);
// int32_t bmProcessDropReq(SBnodeMgmt *pMgmt, SNodeMsg *pRpcMsg);

// void bmInitMsgHandles(SMgmtWrapper *pWrapper);

// int32_t bmStartWorker(SDnode *pDnode);
// void    bmStopWorker(SDnode *pDnode);
// void    bmInitMsgFp(SMnodeMgmt *pMgmt);
// void    bmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
// int32_t bmPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
// int32_t bmPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
// void    bmConsumeChildQueue(SDnode *pDnode, SNodeMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);
// void    bmConsumeParentQueue(SDnode *pDnode, SRpcMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);

// void bmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
// void bmProcessSyncMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
// void bmProcessReadMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_BNODE_INT_H_*/