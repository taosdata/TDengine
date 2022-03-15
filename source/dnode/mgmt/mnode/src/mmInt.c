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
#include "mmInt.h"
#include "dmInt.h"
#include "mmFile.h"
#include "mmMsg.h"
#include "mmWorker.h"

SMnode *mmAcquire(SMnodeMgmt *pMgmt) {
  SMnode *pMnode = NULL;
  int32_t refCount = 0;

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

void mmRelease(SMnodeMgmt *pMgmt, SMnode *pMnode) {
  if (pMnode == NULL) return;

  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release mnode, refCount:%d", refCount);
}

int32_t mmOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SMnode *pMnode = mndOpen(pMgmt->path, pOption);
  if (pMnode == NULL) {
    dError("failed to open mnode since %s", terrstr());
    return -1;
  }

  if (mmStartWorker(pMgmt) != 0) {
    dError("failed to start mnode worker since %s", terrstr());
    mndClose(pMnode);
    mndDestroy(pMgmt->path);
    return -1;
  }

  pMgmt->deployed = 1;
  if (mmWriteFile(pMgmt) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    pMgmt->deployed = 0;
    mmStopWorker(pMgmt);
    mndClose(pMnode);
    mndDestroy(pMgmt->path);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pMnode = pMnode;
  pMgmt->deployed = 1;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("mnode open successfully");
  return 0;
}

int32_t mmAlter(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) {
    dError("failed to alter mnode since %s", terrstr());
    return -1;
  }

  if (mndAlter(pMnode, pOption) != 0) {
    dError("failed to alter mnode since %s", terrstr());
    mmRelease(pMgmt, pMnode);
    return -1;
  }

  mmRelease(pMgmt, pMnode);
  return 0;
}

int32_t mmDrop(SMnodeMgmt *pMgmt) {
  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) {
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (mmWriteFile(pMgmt) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    mmRelease(pMgmt, pMnode);
    dError("failed to drop mnode since %s", terrstr());
    return -1;
  }

  mmRelease(pMgmt, pMnode);
  mmStopWorker(pMgmt);
  pMgmt->deployed = 0;
  mmWriteFile(pMgmt);
  mndClose(pMnode);
  pMgmt->pMnode = NULL;
  mndDestroy(pMgmt->path);

  return 0;
}

static void mmInitOption(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SDnode *pDnode = pMgmt->pDnode;

  pOption->pDnode = pDnode;
  pOption->sendReqToDnodeFp = dndSendReqToDnode;
  pOption->sendReqToMnodeFp = dndSendReqToMnode;
  pOption->sendRedirectRspFp = dmSendRedirectRsp;
  pOption->putReqToMWriteQFp = mmPutMsgToWriteQueue;
  pOption->putReqToMReadQFp = mmPutMsgToReadQueue;
  pOption->dnodeId = dmGetDnodeId(pDnode);
  pOption->clusterId = dmGetClusterId(pDnode);
}

static void mmBuildOptionForDeploy(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SDnode *pDnode = pMgmt->pDnode;

  mmInitOption(pMgmt, pOption);
  pOption->replica = 1;
  pOption->selfIndex = 0;
  SReplica *pReplica = &pOption->replicas[0];
  pReplica->id = 1;
  pReplica->port = pDnode->cfg.serverPort;
  memcpy(pReplica->fqdn, pDnode->cfg.localFqdn, TSDB_FQDN_LEN);

  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

static void mmBuildOptionForOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  mmInitOption(pMgmt, pOption);
  pOption->selfIndex = pMgmt->selfIndex;
  pOption->replica = pMgmt->replica;
  memcpy(&pOption->replicas, pMgmt->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

int32_t mmBuildOptionFromReq(SMnodeMgmt *pMgmt, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate) {
  SDnode *pDnode = pMgmt->pDnode;

  mmInitOption(pMgmt, pOption);
  pOption->dnodeId = dmGetDnodeId(pDnode);
  pOption->clusterId = dmGetClusterId(pDnode);

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

  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  return 0;
}

static void mmCleanup(SMgmtWrapper *pWrapper) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("mnode-mgmt start to cleanup");
  if (pMgmt->pMnode) {
    mmStopWorker(pMgmt);
    mndClose(pMgmt->pMnode);
    pMgmt->pMnode = NULL;
  }
  free(pMgmt);
  pWrapper->pMgmt = NULL;
  dInfo("mnode-mgmt is cleaned up");
}

static int32_t mmInit(SMgmtWrapper *pWrapper) {
  SDnode     *pDnode = pWrapper->pDnode;
  SMnodeMgmt *pMgmt = calloc(1, sizeof(SMnodeMgmt));
  int32_t     code = -1;
  SMnodeOpt   option = {0};

  dInfo("mnode-mgmt start to init");
  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  taosInitRWLatch(&pMgmt->latch);

  if (mmReadFile(pMgmt) != 0) {
    dError("failed to read file since %s", terrstr());
    goto _OVER;
  }

  if (!pMgmt->deployed) {
    dInfo("mnode start to deploy");
    mmBuildOptionForDeploy(pMgmt, &option);
    code = mmOpen(pMgmt, &option);
  } else {
    dInfo("mnode start to open");
    mmBuildOptionForOpen(pMgmt, &option);
    code = mmOpen(pMgmt, &option);
  }

_OVER:
  if (code == 0) {
    dInfo("mnode-mgmt is initialized");
  } else {
    dError("failed to init mnode-mgmtsince %s", terrstr());
    mmCleanup(pWrapper);
  }

  return code;
}

static bool mmDeployRequired(SDnode *pDnode) {
  if (dmGetDnodeId(pDnode) > 0) {
    return false;
  }

  if (dmGetClusterId(pDnode) > 0) {
    return false;
  }

  if (strcmp(pDnode->cfg.localEp, pDnode->cfg.firstEp) != 0) {
    return false;
  }

  return true;
}

static bool mmRequire(SMgmtWrapper *pWrapper) {
  SMnodeMgmt mgmt = {0};
  mgmt.path = pWrapper->path;
  if (mmReadFile(&mgmt) != 0) {
    return false;
  }

  if (mgmt.dropped) {
    dInfo("mnode has been dropped and needs to be deleted");
    mndDestroy(mgmt.path);
    return false;
  }

  if (mgmt.deployed) {
    dInfo("mnode has been deployed");
    return true;
  }

  bool required = mmDeployRequired(pWrapper->pDnode);
  if (required) {
    dInfo("mnode need to be deployed");
  }

  return required;
}

void mmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = mmInit;
  mgmtFp.closeFp = mmCleanup;
  mgmtFp.requiredFp = mmRequire;

  mmInitMsgHandles(pWrapper);
  pWrapper->name = "mnode";
  pWrapper->fp = mgmtFp;
}

int32_t mmGetUserAuth(SMgmtWrapper *pWrapper, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;

  SMnode *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) {
    terrno = TSDB_CODE_APP_NOT_READY;
    dTrace("failed to get user auth since %s", terrstr());
    return -1;
  }

  int32_t code = mndRetriveAuth(pMnode, user, spi, encrypt, secret, ckey);
  mmRelease(pMgmt, pMnode);

  dTrace("user:%s, retrieve auth spi:%d encrypt:%d", user, *spi, *encrypt);
  return code;
}

int32_t mmGetMonitorInfo(SMgmtWrapper *pWrapper, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  SMnode     *pMnode = mmAcquire(pMgmt);
  if (pMnode == NULL) return -1;

  int32_t code = mndGetMonitorInfo(pMnode, pClusterInfo, pVgroupInfo, pGrantInfo);
  mmRelease(pMgmt, pMnode);
  return code;
}