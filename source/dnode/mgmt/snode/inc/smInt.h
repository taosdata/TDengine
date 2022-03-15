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

#include "dndInt.h"

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
int32_t smProcessCreateReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t smProcessDropReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/