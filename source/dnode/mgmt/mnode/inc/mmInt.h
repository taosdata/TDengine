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

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMnodeMgmt {
  int32_t      refCount;
  int8_t       deployed;
  int8_t       dropped;
  int8_t       replica;
  int8_t       selfIndex;
  SReplica     replicas[TSDB_MAX_REPLICA];
  SMnode      *pMnode;
  SProcObj    *pProcess;
  bool         singleProc;
  SRWLatch     latch;
  SDnodeWorker readWorker;
  SDnodeWorker writeWorker;
  SDnodeWorker syncWorker;
} SMnodeMgmt;

// mmInt.h
void mmGetMgmtFp(SMgmtWrapper *pMgmt);

// mmMgmt.h
int32_t mmInit(SDnode *pDnode);
void    mmCleanup(SDnode *pDnode);

// mmHandle.h
int32_t mmProcessCreateReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessAlterReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessDropReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

int32_t mmGetUserAuth(SMgmtWrapper *pWrapper, char *user, char *spi, char *encrypt, char *secret, char *ckey);
int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo);


#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/