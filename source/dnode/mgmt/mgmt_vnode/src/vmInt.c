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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "vmInt.h"

SVnodeObj *vmAcquireVnode(SVnodeMgmt *pMgmt, int32_t vgId) {
  SVnodeObj *pVnode = NULL;
  int32_t    refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  taosHashGetDup(pMgmt->hash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pVnode != NULL) {
    dTrace("vgId:%d, acquire vnode, refCount:%d", pVnode->vgId, refCount);
  }

  return pVnode;
}

void vmReleaseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  if (pVnode == NULL) return;

  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("vgId:%d, release vnode, refCount:%d", pVnode->vgId, refCount);
}

int32_t vmOpenVnode(SVnodeMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl) {
  SVnodeObj *pVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = pCfg->vgId;
  pVnode->refCount = 0;
  pVnode->vgVersion = pCfg->vgVersion;
  pVnode->dropped = 0;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  pVnode->path = tstrdup(pCfg->path);
  pVnode->pImpl = pImpl;

  if (pVnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (vmAllocQueue(pMgmt, pVnode) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  int32_t code = taosHashPut(pMgmt->hash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
  taosWUnLockLatch(&pMgmt->latch);

  return code;
}

void vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  char path[TSDB_FILENAME_LEN] = {0};

  taosWLockLatch(&pMgmt->latch);
  taosHashRemove(pMgmt->hash, &pVnode->vgId, sizeof(int32_t));
  taosWUnLockLatch(&pMgmt->latch);

  vmReleaseVnode(pMgmt, pVnode);
  while (pVnode->refCount > 0) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pWriteQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pSyncQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pApplyQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pQueryQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pFetchQ)) taosMsleep(10);
  while (!taosQueueEmpty(pVnode->pMergeQ)) taosMsleep(10);

  vmFreeQueue(pMgmt, pVnode);
  vnodeClose(pVnode->pImpl);
  pVnode->pImpl = NULL;

  dDebug("vgId:%d, vnode is closed", pVnode->vgId);

  if (pVnode->dropped) {
    dDebug("vgId:%d, vnode is destroyed for dropped:%d", pVnode->vgId, pVnode->dropped);
    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, pVnode->vgId);
    vnodeDestroy(path, pMgmt->pTfs);
  }

  taosMemoryFree(pVnode->path);
  taosMemoryFree(pVnode);
}

static void *vmOpenVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;
  char          path[TSDB_FILENAME_LEN];

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SWrapperCfg *pCfg = &pThread->pCfgs[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pCfg->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-open", stepDesc);

    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, pCfg->vgId);
    SVnode *pImpl = vnodeOpen(path, pMgmt->pTfs, pMgmt->msgCb);
    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      vmOpenVnode(pMgmt, pCfg, pImpl);
      dDebug("vgId:%d, is opened by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->opened++;
      atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
    }
  }

  dDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t vmOpenVnodes(SVnodeMgmt *pMgmt) {
  pMgmt->hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->hash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to init vnode hash since %s", terrstr());
    return -1;
  }

  SWrapperCfg *pCfgs = NULL;
  int32_t      numOfVnodes = 0;
  if (vmGetVnodeListFromFile(pMgmt, &pCfgs, &numOfVnodes) != 0) {
    dInfo("failed to get vnode list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->state.totalVnodes = numOfVnodes;

  int32_t threadNum = 1;  // tsNumOfCores;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].pCfgs = taosMemoryCalloc(vnodesPerThread, sizeof(SWrapperCfg));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    pThread->pCfgs[pThread->vnodeNum++] = pCfgs[v];
  }

  dInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, vmOpenVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->pCfgs);
  }
  taosMemoryFree(threads);
  taosMemoryFree(pCfgs);

  if (pMgmt->state.openVnodes != pMgmt->state.totalVnodes) {
    dError("there are total vnodes:%d, opened:%d", pMgmt->state.totalVnodes, pMgmt->state.openVnodes);
    return -1;
  } else {
    dInfo("total vnodes:%d open successfully", pMgmt->state.totalVnodes);
    return 0;
  }
}

static void vmCloseVnodes(SVnodeMgmt *pMgmt) {
  dInfo("start to close all vnodes");

  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vmCloseVnode(pMgmt, pVnodes[i]);
  }

  if (pVnodes != NULL) {
    taosMemoryFree(pVnodes);
  }

  if (pMgmt->hash != NULL) {
    taosHashCleanup(pMgmt->hash);
    pMgmt->hash = NULL;
  }

  dInfo("total vnodes:%d are all closed", numOfVnodes);
}

static void vmCleanup(SVnodeMgmt *pMgmt) {
  vmCloseVnodes(pMgmt);
  vmStopWorker(pMgmt);
  vnodeCleanup();
  tfsClose(pMgmt->pTfs);
  taosMemoryFree(pMgmt);
}

static int32_t vmInit(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t code = -1;

  SVnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SVnodeMgmt));
  if (pMgmt == NULL) goto _OVER;

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.queueFps[WRITE_QUEUE] = (PutToQueueFp)vmPutRpcMsgToWriteQueue;
  pMgmt->msgCb.queueFps[SYNC_QUEUE] = (PutToQueueFp)vmPutRpcMsgToSyncQueue;
  pMgmt->msgCb.queueFps[APPLY_QUEUE] = (PutToQueueFp)vmPutRpcMsgToApplyQueue;
  pMgmt->msgCb.queueFps[QUERY_QUEUE] = (PutToQueueFp)vmPutRpcMsgToQueryQueue;
  pMgmt->msgCb.queueFps[FETCH_QUEUE] = (PutToQueueFp)vmPutRpcMsgToFetchQueue;
  pMgmt->msgCb.queueFps[MERGE_QUEUE] = (PutToQueueFp)vmPutRpcMsgToMergeQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)vmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;
  taosInitRWLatch(&pMgmt->latch);

  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, tsDataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pMgmt->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pMgmt->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-tfs", "initialized");

  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-wal", "initialized");

  if (syncInit() != 0) {
    dError("failed to open sync since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-sync", "initialized");

  if (vnodeInit(tsNumOfCommitThreads) != 0) {
    dError("failed to init vnode since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-commit", "initialized");

  if (vmStartWorker(pMgmt) != 0) {
    dError("failed to init workers since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-worker", "initialized");

  if (vmOpenVnodes(pMgmt) != 0) {
    dError("failed to open vnode since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("vnode-vnodes", "initialized");

  if (udfcOpen() != 0) {
    dError("failed to open udfc in vnode");
    goto _OVER;
  }

  code = 0;

_OVER:
  if (code == 0) {
    pOutput->pMgmt = pMgmt;
  } else {
    dError("failed to init vnodes-mgmt since %s", terrstr());
    vmCleanup(pMgmt);
  }

  return code;
}

static int32_t vmRequire(const SMgmtInputOpt *pInput, bool *required) {
  *required = tsNumOfSupportVnodes > 0;
  return 0;
}

static int32_t vmStart(SVnodeMgmt *pMgmt) {
  taosRLockLatch(&pMgmt->latch);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    vnodeStart(pVnode->pImpl);
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);
  return 0;
}

static void vmStop(SVnodeMgmt *pMgmt) {
  // process inside the vnode
}

SMgmtFunc vmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = vmInit;
  mgmtFunc.closeFp = (NodeCloseFp)vmCleanup;
  mgmtFunc.startFp = (NodeStartFp)vmStart;
  mgmtFunc.stopFp = (NodeStopFp)vmStop;
  mgmtFunc.requiredFp = vmRequire;
  mgmtFunc.getHandlesFp = vmGetMsgHandles;

  return mgmtFunc;
}
