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
  SDnodeData           *pData;
  SMsgCb                msgCb;
  const char           *path;
  const char           *name;
  SQueryAutoQWorkerPool queryPool;
  SQueryAutoQWorkerPool streamReaderPool;
  SWWorkerPool          fetchPool;
  SSingleWorker         mgmtWorker;
  SSingleWorker         mgmtMultiWorker;
  SHashObj             *runngingHash;
  SHashObj             *closedHash;
  SHashObj             *creatingHash;
  SHashObj             *mountTfsHash;  // key: mountId, value: SMountTfs pointer
  TdThreadRwlock        hashLock;
  TdThreadMutex         mutex;
  SVnodesStat           state;
  STfs                 *pTfs;
  TdThread              thread;
  bool                  stop;
  TdThreadMutex         fileLock;
} SVnodeMgmt;

typedef struct {
  int64_t mountId;
  char    name[TSDB_MOUNT_NAME_LEN];
  char    path[TSDB_MOUNT_PATH_LEN];
} SMountCfg;
typedef struct {
  char      name[TSDB_MOUNT_NAME_LEN];
  char      path[TSDB_MOUNT_PATH_LEN];
  TdFilePtr pFile;
  STfs     *pTfs;
  int32_t   nRef;
} SMountTfs;

typedef struct {
  int32_t vgId;
  int32_t vgVersion;
  int8_t  dropped;
  int32_t diskPrimary;
  int32_t toVgId;
  int64_t mountId;
  char    path[PATH_MAX + 20];
} SWrapperCfg;

typedef struct {
  int32_t      vgId;
  int32_t      vgVersion;
  int32_t      refCount;
  int8_t       dropped;
  int8_t       failed;
  int8_t       disable;
  int32_t      diskPrimary;
  int32_t      toVgId;
  int64_t      mountId;
  char        *path;
  SVnode      *pImpl;
  SMultiWorker pWriteW;
  SMultiWorker pSyncW;
  SMultiWorker pSyncRdW;
  SMultiWorker pApplyW;
  STaosQueue  *pQueryQ;
  STaosQueue  *pFetchQ;
  STaosQueue  *pStreamReaderQ;
} SVnodeObj;

typedef struct {
  int32_t      vnodeNum;
  int32_t      opened;
  int32_t      dropped;
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
void       vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode, bool commitAndRemoveWal, bool keepClosed);
void       vmCleanPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId);
void       vmCloseFailedVnode(SVnodeMgmt *pMgmt, int32_t vgId);
int32_t    vmAcquireMountTfs(SVnodeMgmt *pMgmt, int64_t mountId, const char *mountName, const char *mountPath,
                             STfs **ppTfs);
bool       vmReleaseMountTfs(SVnodeMgmt *pMgmt, int64_t mountId, int32_t minRef);

// vmHandle.c
SArray *vmGetMsgHandles();
int32_t vmProcessCreateVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessDropVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterVnodeReplicaReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessDisableVnodeWriteReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessSetKeepVersionReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterHashRangeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterVnodeTypeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessCheckLearnCatchupReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessArbHeartBeatReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessRetrieveMountPathReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessMountVnodeReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmProcessAlterVnodeElectBaselineReq(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

// vmFile.c
int32_t vmGetVnodeListFromFile(SVnodeMgmt *pMgmt, SWrapperCfg **ppCfgs, int32_t *numOfVnodes);
int32_t vmWriteVnodeListToFile(SVnodeMgmt *pMgmt);
int32_t vmGetVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes);
int32_t vmGetAllVnodeListFromHash(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes);
int32_t vmGetAllVnodeListFromHashWithCreating(SVnodeMgmt *pMgmt, int32_t *numOfVnodes, SVnodeObj ***ppVnodes);
int32_t vmGetMountListFromFile(SVnodeMgmt *pMgmt, SMountCfg **ppCfgs, int32_t *numOfMounts);
int32_t vmWriteMountListToFile(SVnodeMgmt *pMgmt);
int32_t vmGetMountDisks(SVnodeMgmt *pMgmt, const char *mountPath, SArray **ppDisks);
int32_t vmMountCheckRunning(const char *mountName, const char *mountPath, TdFilePtr *pFile, int32_t retryLimit);

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
int32_t vmPutMsgToStreamReaderQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToStreamCtrlQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToStreamLongExecQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

int32_t vmPutMsgToStreamChkQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t vmPutMsgToMultiMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_VNODES_INT_H_*/
