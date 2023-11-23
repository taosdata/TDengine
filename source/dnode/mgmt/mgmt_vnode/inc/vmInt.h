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
  SDnodeData      *pData;
  SMsgCb           msgCb;
  const char      *path;
  const char      *name;
  SQWorkerPool     queryPool;
  SAutoQWorkerPool streamPool;
  SWWorkerPool     fetchPool;
  SSingleWorker    mgmtWorker;
  SHashObj        *hash;
  TdThreadRwlock   lock;
  SVnodesStat      state;
  STfs            *pTfs;
  TdThread         thread;
  bool             stop;
} SVnodeMgmt;

typedef struct {
  int32_t vgId;
  int32_t vgVersion;
  int8_t  dropped;
  int32_t diskPrimary;
  int32_t toVgId;
  char    path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t       vgId;
  int32_t       vgVersion;
  int32_t       refCount;
  int8_t        dropped;
  int8_t        failed;
  int8_t        disable;
  int32_t       diskPrimary;
  int32_t       toVgId;
  char         *path;
  SVnode       *pImpl;
  SMultiWorker  pWriteW;
  SMultiWorker  pSyncW;
  SMultiWorker  pSyncRdW;
  SMultiWorker  pApplyW;
  STaosQueue   *pQueryQ;
  STaosQueue   *pStreamQ;
  STaosQueue   *pFetchQ;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      failed;
  bool         updateVnodesList;
  int32_t      threadIndex;
  TdThread     thread;
  SVnodeMgmt  *pMgmt;
  SWrapperCfg *pCfgs;
  SVnodeObj  **ppVnodes;
} SVnodeThread;

// vmInt.c
int32_t    vmGetPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId);
int32_t    vmAllocPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId);
SVnodeObj *vmAcquireVnode(SVnodeMgmt *pMgmt, int32_t vgId);
SVnodeObj *vmAcquireVnodeImpl(SVnodeMgmt *pMgmt, int32_t vgId, bool strict);
void       vmReleaseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);
int32_t    vmOpenVnode(SVnodeMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl);
void       vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode, bool commitAndRemoveWal);

// vmHandle.c
SArray *vmGetMsgHandles();
int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterVnodeReplicaReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessDisableVnodeWriteReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterHashRangeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterVnodeTypeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessCheckLearnCatchupReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

// vmFile.c
int32_t     vmGetVnodeListFromFile(SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes);
int32_t     vmWriteVnodeListToFile(SVnodeMgmt *pMgmt);
SVnodeObj **vmGetVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes);

// vmWorker.c
int32_t vmStartWorker(SVnodeMgmt *pMgmt);
void    vmStopWorker(SVnodeMgmt *pMgmt);
int32_t vmAllocQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);
void    vmFreeQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode);

int32_t vmGetQueueSize(SVnodeMgmt *pMgmt, int32_t vgId, EQueueType qtype);
int32_t vmPutRpcMsgToQueue(SVnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc);

int32_t vmPutMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToSyncRdQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToStreamQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_INT_H_*/
