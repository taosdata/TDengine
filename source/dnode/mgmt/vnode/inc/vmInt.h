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

#ifndef _TD_DND_VNODE_INT_H_
#define _TD_DND_VNODE_INT_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef struct {
  int32_t openVnodes;
  int32_t totalVnodes;
  int32_t masterNum;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
} SVnodesStat;

typedef struct SVnodesMgmt {
  SVnodesStat  stat;
  SHashObj    *hash;
  SRWLatch     latch;
  SQWorkerPool queryPool;
  SFWorkerPool fetchPool;
  SWWorkerPool syncPool;
  SWWorkerPool writePool;
  STfs        *pTfs;
  SMsgHandle   msgHandles[TDMT_MAX];
  SProcObj    *pProcess;
  bool         singleProc;
} SVnodesMgmt;

void vmGetMgmtFp(SMgmtWrapper *pMgmt) ;

int32_t dndInitVnodes(SDnode *pDnode);
void    dndCleanupVnodes(SDnode *pDnode);
void    vmGetVnodeLoads(SDnode *pDnode, SArray *pLoads);
void    dndProcessVnodeWriteMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeSyncMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeQueryMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);
void    dndProcessVnodeFetchMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet);

int32_t vmProcessCreateVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessAlterVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessDropVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t dndProcessAuthVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessSyncVnodeReq(SDnode *pDnode, SRpcMsg *pReq);
int32_t vmProcessCompactVnodeReq(SDnode *pDnode, SRpcMsg *pReq);

int32_t vmGetTfsMonitorInfo(SMgmtWrapper *pWrapper, SMonDiskInfo *pInfo);
void vmGetVndMonitorInfo(SMgmtWrapper *pWrapper, SMonDnodeInfo *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODE_INT_H_*/