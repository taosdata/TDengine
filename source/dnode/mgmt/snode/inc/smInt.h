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

#include "dnd.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSnodeMgmt {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  int8_t       uniqueWorkerInUse;
  SSnode      *pSnode;
  SRWLatch     latch;
  SArray      *uniqueWorkers;  // SArray<SDnodeWorker*>
  SDnodeWorker sharedWorker;
} SSnodeMgmt;

void smGetMgmtFp(SMgmtWrapper *pMgmt);

int32_t dndInitSnode(SDnode *pDnode);
void    dndCleanupSnode(SDnode *pDnode);

void    dndProcessSnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t smProcessCreateReq(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t smProcessDropReq(SSnodeMgmt *pMgmt, SNodeMsg *pMsg);

void smInitMsgHandles(SMgmtWrapper *pWrapper);

int32_t smStartWorker(SDnode *pDnode);
void    smStopWorker(SDnode *pDnode);
void    smInitMsgFp(SMnodeMgmt *pMgmt);
void    smProcessRpcMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
int32_t smPutMsgToWriteQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t smPutMsgToReadQueue(SDnode *pDnode, SRpcMsg *pRpcMsg);
void    smConsumeChildQueue(SDnode *pDnode, SNodeMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);
void    smConsumeParentQueue(SDnode *pDnode, SRpcMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen);

void smProcessWriteMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void smProcessSyncMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
void smProcessReadMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg);


#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/