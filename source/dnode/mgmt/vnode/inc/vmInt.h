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

#include "sync.h"
#include "vm.h"
#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodesMgmt {
  SHashObj     *hash;
  SRWLatch      latch;
  SVnodesStat   state;
  STfs         *pTfs;
  SQWorkerPool  queryPool;
  SQWorkerPool  fetchPool;
  SWWorkerPool  syncPool;
  SWWorkerPool  writePool;
  const char   *path;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  SDnodeWorker  mgmtWorker;
} SVnodesMgmt;

typedef struct {
  int32_t  vgId;
  int32_t  vgVersion;
  int8_t   dropped;
  uint64_t dbUid;
  char     db[TSDB_DB_FNAME_LEN];
  char     path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t       vgId;
  int32_t       refCount;
  int32_t       vgVersion;
  int8_t        dropped;
  int8_t        accessState;
  uint64_t      dbUid;
  char         *db;
  char         *path;
  SVnode       *pImpl;
  STaosQueue   *pWriteQ;
  STaosQueue   *pSyncQ;
  STaosQueue   *pApplyQ;
  STaosQueue   *pQueryQ;
  STaosQueue   *pFetchQ;
  SMgmtWrapper *pWrapper;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      failed;
  int32_t      threadIndex;
  TdThread     thread;
  SVnodesMgmt *pMgmt;
  SWrapperCfg *pCfgs;
} SVnodeThread;

// vmInt.c
SVnodeObj *vmAcquireVnode(SVnodesMgmt *pMgmt, int32_t vgId);
void       vmReleaseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);
int32_t    vmOpenVnode(SVnodesMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl);
void       vmCloseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);

// vmMsg.c
void    vmInitMsgHandles(SMgmtWrapper *pWrapper);
int32_t vmProcessCreateVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pReq);
int32_t vmProcessAlterVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pReq);
int32_t vmProcessDropVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pReq);
int32_t vmProcessSyncVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pReq);
int32_t vmProcessCompactVnodeReq(SVnodesMgmt *pMgmt, SNodeMsg *pReq);

// vmFile.c
int32_t     vmGetVnodesFromFile(SVnodesMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes);
int32_t     vmWriteVnodesToFile(SVnodesMgmt *pMgmt);
SVnodeObj **vmGetVnodesFromHash(SVnodesMgmt *pMgmt, int32_t *numOfVnodes);

// vmWorker.c
int32_t vmStartWorker(SVnodesMgmt *pMgmt);
void    vmStopWorker(SVnodesMgmt *pMgmt);
int32_t vmAllocQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);
void    vmFreeQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode);

int32_t vmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);
int32_t vmPutMsgToFetchQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);
int32_t vmPutMsgToApplyQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg);

int32_t vmProcessWriteMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);
int32_t vmProcessSyncMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);
int32_t vmProcessQueryMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);
int32_t vmProcessFetchMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);
int32_t vmProcessMgmtMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_INT_H_*/