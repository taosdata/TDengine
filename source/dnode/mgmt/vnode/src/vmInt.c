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

SVnodeObj *vmAcquireVnode(SVnodesMgmt *pMgmt, int32_t vgId) {
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

void vmReleaseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
  if (pVnode == NULL) return;

  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("vgId:%d, release vnode, refCount:%d", pVnode->vgId, refCount);
}

int32_t vmOpenVnode(SVnodesMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl) {
  SVnodeObj *pVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = pCfg->vgId;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  pVnode->pWrapper = pMgmt->pWrapper;
  pVnode->pImpl = pImpl;
  pVnode->vgVersion = pCfg->vgVersion;
  pVnode->dbUid = pCfg->dbUid;
  pVnode->db = tstrdup(pCfg->db);
  pVnode->path = tstrdup(pCfg->path);

  if (pVnode->path == NULL || pVnode->db == NULL) {
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

  if (code != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return code;
}

void vmCloseVnode(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
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

  vmFreeQueue(pMgmt, pVnode);
  vnodeClose(pVnode->pImpl);
  pVnode->pImpl = NULL;

  dDebug("vgId:%d, vnode is closed", pVnode->vgId);

  if (pVnode->dropped) {
    dDebug("vgId:%d, vnode is destroyed for dropped:%d", pVnode->vgId, pVnode->dropped);
    vnodeDestroy(pVnode->path);
  }

  taosMemoryFree(pVnode->path);
  taosMemoryFree(pVnode->db);
  taosMemoryFree(pVnode);
}

static void *vmOpenVnodeFunc(void *param) {
  SVnodeThread *pThread = param;
  SVnodesMgmt  *pMgmt = pThread->pMgmt;
  SDnode       *pDnode = pMgmt->pDnode;

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SWrapperCfg *pCfg = &pThread->pCfgs[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pCfg->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    dndReportStartup(pDnode, "open-vnodes", stepDesc);

    SMsgCb msgCb = {0};
    msgCb.pWrapper = pMgmt->pWrapper;
    msgCb.queueFps[QUERY_QUEUE] = vmPutMsgToQueryQueue;
    msgCb.queueFps[FETCH_QUEUE] = vmPutMsgToFetchQueue;
    msgCb.queueFps[APPLY_QUEUE] = vmPutMsgToApplyQueue;
    msgCb.qsizeFp = vmGetQueueSize;
    msgCb.sendReqFp = dndSendReqToDnode;
    msgCb.sendMnodeReqFp = dndSendReqToMnode;
    msgCb.sendRspFp = dndSendRsp;
    SVnodeCfg cfg = {.msgCb = msgCb, .pTfs = pMgmt->pTfs, .vgId = pCfg->vgId, .dbId = pCfg->dbUid};
    SVnode   *pImpl = vnodeOpen(pCfg->path, &cfg);
    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      vmOpenVnode(pMgmt, pCfg, pImpl);
      dDebug("vgId:%d, is opened by thread:%d", pCfg->vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
  }

  dDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t vmOpenVnodes(SVnodesMgmt *pMgmt) {
  SDnode *pDnode = pMgmt->pDnode;
  taosInitRWLatch(&pMgmt->latch);

  pMgmt->hash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->hash == NULL) {
    dError("failed to init vnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SWrapperCfg *pCfgs = NULL;
  int32_t      numOfVnodes = 0;
  if (vmGetVnodesFromFile(pMgmt, &pCfgs, &numOfVnodes) != 0) {
    dInfo("failed to get vnode list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->state.totalVnodes = numOfVnodes;

#if 0 
  int32_t threadNum = tsNumOfCores;
#else
  int32_t threadNum = 1;
#endif
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
    if (taosThreadCreate(&pThread->thread, &thAttr, vmOpenVnodeFunc, pThread) != 0) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
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

static void vmCloseVnodes(SVnodesMgmt *pMgmt) {
  dInfo("start to close all vnodes");

  int32_t     numOfVnodes = 0;
  SVnodeObj **pVnodes = vmGetVnodesFromHash(pMgmt, &numOfVnodes);

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

static void vmCleanup(SMgmtWrapper *pWrapper) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("vnode-mgmt start to cleanup");
  vmCloseVnodes(pMgmt);
  vmStopWorker(pMgmt);
  vnodeCleanup();
  // walCleanUp();
  taosMemoryFree(pMgmt);
  pWrapper->pMgmt = NULL;
  dInfo("vnode-mgmt is cleaned up");
}

static int32_t vmInit(SMgmtWrapper *pWrapper) {
  SDnode      *pDnode = pWrapper->pDnode;
  SVnodesMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SVnodesMgmt));
  int32_t      code = -1;

  dInfo("vnode-mgmt start to init");
  if (pMgmt == NULL) goto _OVER;

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  taosInitRWLatch(&pMgmt->latch);

  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, pDnode->dataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = pDnode->pDisks;
  int32_t   numOfDisks = pDnode->numOfDisks;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pMgmt->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pMgmt->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    goto _OVER;
  }

  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    goto _OVER;
  }

  if (vnodeInit() != 0) {
    dError("failed to init vnode since %s", terrstr());
    goto _OVER;
  }

  if (vmStartWorker(pMgmt) != 0) {
    dError("failed to init workers since %s", terrstr()) goto _OVER;
  }

  if (vmOpenVnodes(pMgmt) != 0) {
    dError("failed to open vnode since %s", terrstr());
    return -1;
  }

  code = 0;

_OVER:
  if (code == 0) {
    pWrapper->pMgmt = pMgmt;
    dInfo("vnodes-mgmt is initialized");
  } else {
    dError("failed to init vnodes-mgmt since %s", terrstr());
    vmCleanup(pWrapper);
  }

  return 0;
}

static int32_t vmRequire(SMgmtWrapper *pWrapper, bool *required) {
  SDnode *pDnode = pWrapper->pDnode;
  *required = pDnode->numOfSupportVnodes > 0;
  return 0;
}

void vmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = vmInit;
  mgmtFp.closeFp = vmCleanup;
  mgmtFp.requiredFp = vmRequire;

  vmInitMsgHandles(pWrapper);
  pWrapper->name = "vnode";
  pWrapper->fp = mgmtFp;
}

int32_t vmMonitorTfsInfo(SMgmtWrapper *pWrapper, SMonDiskInfo *pInfo) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return -1;

  return tfsGetMonitorInfo(pMgmt->pTfs, pInfo);
}

void vmMonitorVnodeReqs(SMgmtWrapper *pWrapper, SMonDnodeInfo *pInfo) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  SVnodesStat *pStat = &pMgmt->state;
  pInfo->req_select = pStat->numOfSelectReqs;
  pInfo->req_insert = pStat->numOfInsertReqs;
  pInfo->req_insert_success = pStat->numOfInsertSuccessReqs;
  pInfo->req_insert_batch = pStat->numOfBatchInsertReqs;
  pInfo->req_insert_batch_success = pStat->numOfBatchInsertSuccessReqs;
  pInfo->errors = tsNumOfErrorLogs;
  pInfo->vnodes_num = pStat->totalVnodes;
  pInfo->masters = pStat->masterNum;
}

void vmMonitorVnodeLoads(SMgmtWrapper *pWrapper, SArray *pLoads) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  SVnodesStat *pStat = &pMgmt->state;
  int32_t      totalVnodes = 0;
  int32_t      masterNum = 0;
  int64_t      numOfSelectReqs = 0;
  int64_t      numOfInsertReqs = 0;
  int64_t      numOfInsertSuccessReqs = 0;
  int64_t      numOfBatchInsertReqs = 0;
  int64_t      numOfBatchInsertSuccessReqs = 0;

  taosRLockLatch(&pMgmt->latch);

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    if (ppVnode == NULL || *ppVnode == NULL) continue;

    SVnodeObj *pVnode = *ppVnode;
    SVnodeLoad vload = {0};
    vnodeGetLoad(pVnode->pImpl, &vload);
    taosArrayPush(pLoads, &vload);

    numOfSelectReqs += vload.numOfSelectReqs;
    numOfInsertReqs += vload.numOfInsertReqs;
    numOfInsertSuccessReqs += vload.numOfInsertSuccessReqs;
    numOfBatchInsertReqs += vload.numOfBatchInsertReqs;
    numOfBatchInsertSuccessReqs += vload.numOfBatchInsertSuccessReqs;
    totalVnodes++;
    if (vload.role == TAOS_SYNC_STATE_LEADER) masterNum++;

    pIter = taosHashIterate(pMgmt->hash, pIter);
  }

  taosRUnLockLatch(&pMgmt->latch);

  pStat->totalVnodes = totalVnodes;
  pStat->masterNum = masterNum;
  pStat->numOfSelectReqs = numOfSelectReqs;
  pStat->numOfInsertReqs = numOfInsertReqs;
  pStat->numOfInsertSuccessReqs = numOfInsertSuccessReqs;
  pStat->numOfBatchInsertReqs = numOfBatchInsertReqs;
  pStat->numOfBatchInsertSuccessReqs = numOfBatchInsertSuccessReqs;
}