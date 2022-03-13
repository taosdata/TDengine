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
#include "mm.h"

#include "dndMgmt.h"
#include "dndTransport.h"

static void    mmInitOption(SDnode *pDnode, SMnodeOpt *pOption);
static void    mmBuildOptionForDeploy(SDnode *pDnode, SMnodeOpt *pOption);
static void    mmBuildOptionForOpen(SDnode *pDnode, SMnodeOpt *pOption);
static bool    mmDeployRequired(SDnode *pDnode);
static int32_t mmOpenImp(SDnode *pDnode, SMnodeOpt *pOption);

int32_t mmInit(SDnode *pDnode) {
  dInfo("mnode mgmt start to init");
  int32_t code = -1;

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  taosInitRWLatch(&pMgmt->latch);
  mmInitMsgFp(pMgmt);

  if (mmReadFile(pDnode) != 0) {
    goto _OVER;
  }

  if (pMgmt->dropped) {
    dInfo("mnode has been dropped and needs to be deleted");
    mndDestroy(pDnode->dir.mnode);
    code = 0;
    goto _OVER;
  }

  if (!pMgmt->deployed) {
    bool required = mmDeployRequired(pDnode);
    if (!required) {
      dInfo("mnode does not need to be deployed");
      code = 0;
      goto _OVER;
    }

    dInfo("mnode start to deploy");
    SMnodeOpt option = {0};
    mmBuildOptionForDeploy(pDnode, &option);
    code = mmOpen(pDnode, &option);
  } else {
    dInfo("mnode start to open");
    SMnodeOpt option = {0};
    mmBuildOptionForOpen(pDnode, &option);
    code = mmOpen(pDnode, &option);
  }

_OVER:
  if (code == 0) {
    dInfo("mnode mgmt init success");
  } else {
    dError("failed to init mnode mgmt since %s", terrstr());
    mmCleanup(pDnode);
  }

  return code;
}

void mmCleanup(SDnode *pDnode) {
  dInfo("mnode mgmt start to clean up");
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  if (pMgmt->pMnode) {
    mmStopWorker(pDnode);
    mndClose(pMgmt->pMnode);
    pMgmt->pMnode = NULL;
  }
  dInfo("mnode mgmt is cleaned up");
}

SMnode *mmAcquire(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  SMnode     *pMnode = NULL;
  int32_t     refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pMnode = pMgmt->pMnode;
  } else {
    terrno = TSDB_CODE_DND_MNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pMnode != NULL) {
    dTrace("acquire mnode, refCount:%d", refCount);
  }
  return pMnode;
}

void mmRelease(SDnode *pDnode, SMnode *pMnode) {
  if (pMnode == NULL) return;

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release mnode, refCount:%d", refCount);
}

int32_t mmOpen(SDnode *pDnode, SMnodeOpt *pOption) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->singleProc = true;

  int32_t code = mmOpenImp(pDnode, pOption);

  if (code == 0 && !pMgmt->singleProc) {
    SProcCfg cfg = {.childQueueSize = 1024 * 1024,
                    .childConsumeFp = (ProcConsumeFp)mmConsumeChildQueue,
                    .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                    .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                    .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                    .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                    .parentQueueSize = 1024 * 1024,
                    .parentConsumeFp = (ProcConsumeFp)mmConsumeParentQueue,
                    .parentdMallocHeadFp = (ProcMallocFp)malloc,
                    .parentFreeHeadFp = (ProcFreeFp)free,
                    .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                    .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                    .testFlag = 0,
                    .pParent = pDnode,
                    .name = "mnode"};

    pMgmt->pProcess = taosProcInit(&cfg);
    if (pMgmt->pProcess == NULL) {
      return -1;
    }

    return taosProcRun(pMgmt->pProcess);
  }

  return code;
}

int32_t mmAlter(SDnode *pDnode, SMnodeOpt *pOption) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) {
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  if (mndAlter(pMnode, pOption) != 0) {
    dError("failed to alter mnode since %s", terrstr());
    mmRelease(pDnode, pMnode);
    return -1;
  }

  mmRelease(pDnode, pMnode);
  return 0;
}

int32_t mmDrop(SDnode *pDnode) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mmAcquire(pDnode);
  if (pMnode == NULL) {
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (mmWriteFile(pDnode) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    mmRelease(pDnode, pMnode);
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  mmRelease(pDnode, pMnode);
  mmStopWorker(pDnode);
  pMgmt->deployed = 0;
  mmWriteFile(pDnode);
  mndClose(pMnode);
  pMgmt->pMnode = NULL;
  mndDestroy(pDnode->dir.mnode);

  return 0;
}

static bool mmDeployRequired(SDnode *pDnode) {
  if (dndGetDnodeId(pDnode) > 0) {
    return false;
  }

  if (dndGetClusterId(pDnode) > 0) {
    return false;
  }

  if (strcmp(pDnode->cfg.localEp, pDnode->cfg.firstEp) != 0) {
    return false;
  }

  return true;
}

static void mmInitOption(SDnode *pDnode, SMnodeOpt *pOption) {
  pOption->pDnode = pDnode;
  pOption->sendReqToDnodeFp = dndSendReqToDnode;
  pOption->sendReqToMnodeFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dndSendRedirectRsp;
  pOption->putReqToMWriteQFp = mmPutMsgToWriteQueue;
  pOption->putReqToMReadQFp = mmPutMsgToReadQueue;
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);
}

static void mmBuildOptionForDeploy(SDnode *pDnode, SMnodeOpt *pOption) {
  mmInitOption(pDnode, pOption);
  pOption->replica = 1;
  pOption->selfIndex = 0;
  SReplica *pReplica = &pOption->replicas[0];
  pReplica->id = 1;
  pReplica->port = pDnode->cfg.serverPort;
  memcpy(pReplica->fqdn, pDnode->cfg.localFqdn, TSDB_FQDN_LEN);

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

static void mmBuildOptionForOpen(SDnode *pDnode, SMnodeOpt *pOption) {
  mmInitOption(pDnode, pOption);
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pOption->selfIndex = pMgmt->selfIndex;
  pOption->replica = pMgmt->replica;
  memcpy(&pOption->replicas, pMgmt->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

int32_t mmBuildOptionFromReq(SDnode *pDnode, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate) {
  mmInitOption(pDnode, pOption);
  pOption->dnodeId = dndGetDnodeId(pDnode);
  pOption->clusterId = dndGetClusterId(pDnode);

  pOption->replica = pCreate->replica;
  pOption->selfIndex = -1;
  for (int32_t i = 0; i < pCreate->replica; ++i) {
    SReplica *pReplica = &pOption->replicas[i];
    pReplica->id = pCreate->replicas[i].id;
    pReplica->port = pCreate->replicas[i].port;
    memcpy(pReplica->fqdn, pCreate->replicas[i].fqdn, TSDB_FQDN_LEN);
    if (pReplica->id == pOption->dnodeId) {
      pOption->selfIndex = i;
    }
  }

  if (pOption->selfIndex == -1) {
    dError("failed to build mnode options since %s", terrstr());
    return -1;
  }

  SMnodeMgmt *pMgmt = &pDnode->mmgmt;
  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  return 0;
}

static int32_t mmOpenImp(SDnode *pDnode, SMnodeOpt *pOption) {
  SMnodeMgmt *pMgmt = &pDnode->mmgmt;

  SMnode *pMnode = mndOpen(pDnode->dir.mnode, pOption);
  if (pMnode == NULL) {
    dError("failed to open mnode since %s", terrstr());
    return -1;
  }

  if (mmStartWorker(pDnode) != 0) {
    dError("failed to start mnode worker since %s", terrstr());
    mndClose(pMnode);
    mndDestroy(pDnode->dir.mnode);
    return -1;
  }

  pMgmt->deployed = 1;
  if (mmWriteFile(pDnode) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    pMgmt->deployed = 0;
    mmStopWorker(pDnode);
    mndClose(pMnode);
    mndDestroy(pDnode->dir.mnode);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pMnode = pMnode;
  pMgmt->deployed = 1;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("mnode open successfully");
  return 0;
}
