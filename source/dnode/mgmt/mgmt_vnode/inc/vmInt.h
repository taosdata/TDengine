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

#ifndef _TD_DND_VNODES_INT_H_
#define _TD_DND_VNODES_INT_H_

#include "dmUtil.h"

#include "sync.h"
#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeMgmt {
  SDnodeData   *pData;
  SMsgCb        msgCb;
  const char   *path;
  const char   *name;
  SQWorkerPool  queryPool;
  SQWorkerPool  fetchPool;
  SWWorkerPool  syncPool;
  SWWorkerPool  writePool;
  SWWorkerPool  mergePool;
  SSingleWorker mgmtWorker;
  SSingleWorker monitorWorker;
  SHashObj     *hash;
  SRWLatch      latch;
  SVnodesStat   state;
  STfs         *pTfs;
} SVnodeMgmt;

typedef struct {
  int32_t  vgId;
  int32_t  vgVersion;
  int8_t   dropped;
  char     path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t       vgId;
  int32_t       refCount;
  int32_t       vgVersion;
  int8_t        dropped;
  int8_t        accessState;
  char         *path;
  SVnode       *pImpl;
  STaosQueue   *pWriteQ;
  STaosQueue   *pSyncQ;
  STaosQueue   *pApplyQ;
  STaosQueue   *pQueryQ;
  STaosQueue   *pFetchQ;
  STaosQueue   *pMergeQ;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      failed;
  int32_t      threadIndex;
  TdThread     thread;
  SVnodeMgmt  *pMgmt;
  SWrapperCfg *pCfgs;
} SVnodeThread;

// vmInt.c
SVnodeObj *vmAcquireVnode(SVnodeMgmt *pMgmt, int32_t vgId);
void       vmReleaseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);
int32_t    vmOpenVnode(SVnodeMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl);
void       vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);

// vmHandle.c
SArray *vmGetMsgHandles();
int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pReq);
int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pReq);
int32_t vmProcessGetMonitorInfoReq(SVnodeMgmt *pMgmt, SRpcMsg *pReq);
int32_t vmProcessGetLoadsReq(SVnodeMgmt *pMgmt, SRpcMsg *pReq);

// vmFile.c
int32_t     vmGetVnodeListFromFile(SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes);
int32_t     vmWriteVnodeListToFile(SVnodeMgmt *pMgmt);
SVnodeObj **vmGetVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes);

// vmWorker.c
int32_t vmStartWorker(SVnodeMgmt *pMgmt);
void    vmStopWorker(SVnodeMgmt *pMgmt);
int32_t vmAllocQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);
void    vmFreeQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);

int32_t vmPutRpcMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutRpcMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutRpcMsgToApplyQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutRpcMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutRpcMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutRpcMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmGetQueueSize(SVnodeMgmt *pMgmt, int32_t vgId, EQueueType qtype);

int32_t vmPutNodeMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutNodeMsgToMonitorQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_INT_H_*/
