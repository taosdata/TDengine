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

  taosThreadRwlockRdlock(&pMgmt->lock);
  taosHashGetDup(pMgmt->hash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL || pVnode->dropped) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    pVnode = NULL;
  } else {
    int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
    // dTrace("vgId:%d, acquire vnode, ref:%d", pVnode->vgId, refCount);
  }
  taosThreadRwlockUnlock(&pMgmt->lock);

  return pVnode;
}

void vmReleaseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  if (pVnode == NULL) return;

  taosThreadRwlockRdlock(&pMgmt->lock);
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  // dTrace("vgId:%d, release vnode, ref:%d", pVnode->vgId, refCount);
  taosThreadRwlockUnlock(&pMgmt->lock);
}

int32_t vmOpenVnode(SVnodeMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl) {
  SVnodeObj *pVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = pCfg->vgId;
  pVnode->vgVersion = pCfg->vgVersion;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->path = taosStrdup(pCfg->path);
  pVnode->pImpl = pImpl;

  if (pVnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pVnode);
    return -1;
  }

  if (vmAllocQueue(pMgmt, pVnode) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pVnode->path);
    taosMemoryFree(pVnode);
    return -1;
  }

  taosThreadRwlockWrlock(&pMgmt->lock);
  int32_t code = taosHashPut(pMgmt->hash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));
  taosThreadRwlockUnlock(&pMgmt->lock);

  return code;
}

void vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode, bool commitAndRemoveWal) {
  char path[TSDB_FILENAME_LEN] = {0};

  taosThreadRwlockWrlock(&pMgmt->lock);
  taosHashRemove(pMgmt->hash, &pVnode->vgId, sizeof(int32_t));
  taosThreadRwlockUnlock(&pMgmt->lock);
  vmReleaseVnode(pMgmt, pVnode);

  dInfo("vgId:%d, pre close", pVnode->vgId);
  vnodePreClose(pVnode->pImpl);

  dInfo("vgId:%d, wait for vnode ref become 0", pVnode->vgId);
  while (pVnode->refCount > 0) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode write queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pWriteW.queue,
        pVnode->pWriteW.queue->threadId);
  tMultiWorkerCleanup(&pVnode->pWriteW);

  dInfo("vgId:%d, wait for vnode sync queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pSyncW.queue,
        pVnode->pSyncW.queue->threadId);
  tMultiWorkerCleanup(&pVnode->pSyncW);

  dInfo("vgId:%d, wait for vnode sync ctrl queue:%p is empty, thread:%08" PRId64, pVnode->vgId,
        pVnode->pSyncCtrlW.queue, pVnode->pSyncCtrlW.queue->threadId);
  tMultiWorkerCleanup(&pVnode->pSyncCtrlW);

  dInfo("vgId:%d, wait for vnode apply queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pApplyW.queue,
        pVnode->pApplyW.queue->threadId);
  tMultiWorkerCleanup(&pVnode->pApplyW);

  dInfo("vgId:%d, wait for vnode query queue:%p is empty", pVnode->vgId, pVnode->pQueryQ);
  while (!taosQueueEmpty(pVnode->pQueryQ)) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode fetch queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pFetchQ,
        pVnode->pFetchQ->threadId);
  while (!taosQueueEmpty(pVnode->pFetchQ)) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode stream queue:%p is empty", pVnode->vgId, pVnode->pStreamQ);
  while (!taosQueueEmpty(pVnode->pStreamQ)) taosMsleep(10);

  dInfo("vgId:%d, all vnode queues is empty", pVnode->vgId);

  dInfo("vgId:%d, post close", pVnode->vgId);
  vnodePostClose(pVnode->pImpl);

  vmFreeQueue(pMgmt, pVnode);

  if (commitAndRemoveWal) {
    dInfo("vgId:%d, commit data for vnode split", pVnode->vgId);
    vnodeSyncCommit(pVnode->pImpl);
    vnodeBegin(pVnode->pImpl);
    dInfo("vgId:%d, commit data finished", pVnode->vgId);
  }

  vnodeClose(pVnode->pImpl);
  pVnode->pImpl = NULL;
  dInfo("vgId:%d, vnode is closed", pVnode->vgId);

  if (commitAndRemoveWal) {
    char path[TSDB_FILENAME_LEN] = {0};
    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d%swal", TD_DIRSEP, pVnode->vgId, TD_DIRSEP);
    dInfo("vgId:%d, remove all wals, path:%s", pVnode->vgId, path);
    tfsRmdir(pMgmt->pTfs, path);
    tfsMkdir(pMgmt->pTfs, path);
  }

  if (pVnode->dropped) {
    dInfo("vgId:%d, vnode is destroyed, dropped:%d", pVnode->vgId, pVnode->dropped);
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

  dInfo("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
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
      dInfo("vgId:%d, is opened by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->opened++;
      atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
    }
  }

  dInfo("thread:%d, numOfVnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
        pThread->failed);
  return NULL;
}

static int32_t vmOpenVnodes(SVnodeMgmt *pMgmt) {
  pMgmt->hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
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

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
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

  dInfo("open %d vnodes with %d threads", numOfVnodes, threadNum);

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
    dInfo("successfully opened %d vnodes", pMgmt->state.totalVnodes);
    return 0;
  }
}

static void *vmCloseVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;

  dInfo("thread:%d, start to close %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("close-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = pThread->ppVnodes[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to close, %d of %d have been closed", pVnode->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-close", stepDesc);

    vmCloseVnode(pMgmt, pVnode, false);
  }

  dInfo("thread:%d, numOfVnodes:%d is closed", pThread->threadIndex, pThread->vnodeNum);
  return NULL;
}

static void vmCloseVnodes(SVnodeMgmt *pMgmt) {
  dInfo("start to close all vnodes");
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dInfo("vnodes mgmt worker is stopped");

  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].ppVnodes = taosMemoryCalloc(vnodesPerThread, sizeof(SVnode *));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    if (pThread->ppVnodes != NULL && ppVnodes != NULL) {
      pThread->ppVnodes[pThread->vnodeNum++] = ppVnodes[v];
    }
  }

  pMgmt->state.openVnodes = 0;
  dInfo("close %d vnodes with %d threads", numOfVnodes, threadNum);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, vmCloseVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to close vnode since %s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->ppVnodes);
  }
  taosMemoryFree(threads);

  if (ppVnodes != NULL) {
    taosMemoryFree(ppVnodes);
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
  taosThreadRwlockDestroy(&pMgmt->lock);
  taosMemoryFree(pMgmt);
}

static void vmCheckSyncTimeout(SVnodeMgmt *pMgmt) {
  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);

  if (ppVnodes != NULL) {
    for (int32_t i = 0; i < numOfVnodes; ++i) {
      SVnodeObj *pVnode = ppVnodes[i];
      vnodeSyncCheckTimeout(pVnode->pImpl);
      vmReleaseVnode(pMgmt, pVnode);
    }
    taosMemoryFree(ppVnodes);
  }
}

static void *vmThreadFp(void *param) {
  SVnodeMgmt *pMgmt = param;
  int64_t     lastTime = 0;
  setThreadName("vnode-timer");

  while (1) {
    lastTime++;
    taosMsleep(100);
    if (pMgmt->stop) break;
    if (lastTime % 10 != 0) continue;

    int64_t sec = lastTime / 10;
    if (sec % (VNODE_TIMEOUT_SEC / 2) == 0) {
      vmCheckSyncTimeout(pMgmt);
    }
  }

  return NULL;
}

static int32_t vmInitTimer(SVnodeMgmt *pMgmt) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->thread, &thAttr, vmThreadFp, pMgmt) != 0) {
    dError("failed to create vnode timer thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void vmCleanupTimer(SVnodeMgmt *pMgmt) {
  pMgmt->stop = true;
  if (taosCheckPthreadValid(pMgmt->thread)) {
    taosThreadJoin(pMgmt->thread, NULL);
    taosThreadClear(&pMgmt->thread);
  }
}

static int32_t vmInit(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t code = -1;

  SVnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SVnodeMgmt));
  if (pMgmt == NULL) goto _OVER;

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)vmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)vmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;
  taosThreadRwlockInit(&pMgmt->lock, NULL);

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

static void *vmRestoreVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;

  dInfo("thread:%d, start to restore %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("restore-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = pThread->ppVnodes[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been restored", pVnode->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-restore", stepDesc);

    int32_t code = vnodeStart(pVnode->pImpl);
    if (code != 0) {
      dError("vgId:%d, failed to restore vnode by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dInfo("vgId:%d, is restored by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->opened++;
      atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
    }
  }

  dInfo("thread:%d, numOfVnodes:%d, restored:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
        pThread->failed);
  return NULL;
}

static int32_t vmStartVnodes(SVnodeMgmt *pMgmt) {
  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = vmGetVnodeListFromHash(pMgmt, &numOfVnodes);

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].ppVnodes = taosMemoryCalloc(vnodesPerThread, sizeof(SVnode *));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    if (pThread->ppVnodes != NULL && ppVnodes != NULL) {
      pThread->ppVnodes[pThread->vnodeNum++] = ppVnodes[v];
    }
  }

  pMgmt->state.openVnodes = 0;
  dInfo("restore %d vnodes with %d threads", numOfVnodes, threadNum);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, vmRestoreVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to restore vnode since %s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->ppVnodes);
  }
  taosMemoryFree(threads);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (ppVnodes == NULL || ppVnodes[i] == NULL) continue;
    vmReleaseVnode(pMgmt, ppVnodes[i]);
  }

  if (ppVnodes != NULL) {
    taosMemoryFree(ppVnodes);
  }

  return vmInitTimer(pMgmt);
}

static void vmStop(SVnodeMgmt *pMgmt) { vmCleanupTimer(pMgmt); }

SMgmtFunc vmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = vmInit;
  mgmtFunc.closeFp = (NodeCloseFp)vmCleanup;
  mgmtFunc.startFp = (NodeStartFp)vmStartVnodes;
  mgmtFunc.stopFp = (NodeStopFp)vmStop;
  mgmtFunc.requiredFp = vmRequire;
  mgmtFunc.getHandlesFp = vmGetMsgHandles;

  return mgmtFunc;
}
