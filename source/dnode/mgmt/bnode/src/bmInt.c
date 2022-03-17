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
#include "bmInt.h"

SBnode *bmAcquire(SBnodeMgmt *pMgmt) {
  SBnode *pBnode = NULL;
  int32_t refCount = 0;

  taosRLockLatch(&pMgmt->latch);
  if (pMgmt->deployed && !pMgmt->dropped && pMgmt->pBnode != NULL) {
    refCount = atomic_add_fetch_32(&pMgmt->refCount, 1);
    pBnode = pMgmt->pBnode;
  } else {
    terrno = TSDB_CODE_DND_BNODE_NOT_DEPLOYED;
  }
  taosRUnLockLatch(&pMgmt->latch);

  if (pBnode != NULL) {
    dTrace("acquire bnode, refCount:%d", refCount);
  }
  return pBnode;
}

void bmRelease(SBnodeMgmt *pMgmt, SBnode *pBnode) {
  if (pBnode == NULL) return;

  taosRLockLatch(&pMgmt->latch);
  int32_t refCount = atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosRUnLockLatch(&pMgmt->latch);
  dTrace("release bnode, refCount:%d", refCount);
}

static bool bmRequire(SMgmtWrapper *pWrapper) {
  SBnodeMgmt mgmt = {0};
  mgmt.path = pWrapper->path;
  if (mmReadFile(&mgmt) != 0) {
    return false;
  }

  if (mgmt.dropped) {
    dInfo("bnode has been dropped and needs to be deleted");
    mndDestroy(mgmt.path);
    return false;
  }

  if (mgmt.deployed) {
    dInfo("bnode has been deployed");
    return true;
  }

  bool required = mmDeployRequired(pWrapper->pDnode);
  if (required) {
    dInfo("bnode need to be deployed");
  }

  return required;
}

static void bmInitOption(SBnodeMgmt *pMgmt, SBnodeOpt *pOption) {
  SDnode *pDnode = pMgmt->pDnode;

  pOption->pWrapper = pMgmt->pWrapper;
  pOption->sendReqFp = dndSendReqToDnode;
  pOption->sendMnodeReqFp = dndSendReqToMnode;
  pOption->dnodeId = pDnode->dnodeId;
  pOption->clusterId = pDnode->clusterId;
}

int32_t bmOpen(SBnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SDnode *pDnode = pMgmt->pDnode;

  SBnode *pBnode = bmAcquire(pDnode);
  if (pBnode != NULL) {
    bmRelease(pDnode, pBnode);
    terrno = TSDB_CODE_DND_BNODE_ALREADY_DEPLOYED;
    dError("failed to create bnode since %s", terrstr());
    return -1;
  }

  pBnode = bndOpen(pMgmt->path, pOption);
  if (pBnode == NULL) {
    dError("failed to open bnode since %s", terrstr());
    return -1;
  }

  if (bmStartWorker(pDnode) != 0) {
    dError("failed to start bnode worker since %s", terrstr());
    bndClose(pBnode);
    bndDestroy(pMgmt->path);
    return -1;
  }

  pMgmt->deployed = 1;
  if (bmWriteFile(pDnode) != 0) {
    dError("failed to write bnode file since %s", terrstr());
    pMgmt->deployed = 0;
    bmStopWorker(pDnode);
    bndClose(pBnode);
    bndDestroy(pMgmt->path);
    return -1;
  }

  taosWLockLatch(&pMgmt->latch);
  pMgmt->pBnode = pBnode;
  pMgmt->deployed = 1;
  taosWUnLockLatch(&pMgmt->latch);

  dInfo("bnode open successfully");
  return 0;
}

int32_t bmDrop(SBnodeMgmt *pMgmt) {
  SBnode *pBnode = bmAcquire(pMgmt);
  if (pBnode == NULL) {
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  }

  taosRLockLatch(&pMgmt->latch);
  pMgmt->dropped = 1;
  taosRUnLockLatch(&pMgmt->latch);

  if (bmWriteFile(pMgmt) != 0) {
    taosRLockLatch(&pMgmt->latch);
    pMgmt->dropped = 0;
    taosRUnLockLatch(&pMgmt->latch);

    bmRelease(pMgmt, pBnode);
    dError("failed to drop bnode since %s", terrstr());
    return -1;
  }

  bmRelease(pMgmt, pBnode);
  bmStopWorker(pMgmt);
  pMgmt->deployed = 0;
  bmWriteFile(pMgmt);
  bndClose(pBnode);
  pMgmt->pBnode = NULL;
  bndDestroy(pMgmt->path);

  return 0;
}

static int32_t bmInit(SMgmtWrapper *pWrapper) {
  SDnode     *pDnode = pWrapper->pDnode;
  SBnodeMgmt *pMgmt = calloc(1, sizeof(SBnodeMgmt));
  int32_t     code = -1;
  SBnodeOpt   option = {0};

  dInfo("bnode-mgmt start to init");
  if (pMgmt == NULL) goto _OVER;

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  taosInitRWLatch(&pMgmt->latch);

  if (bmReadFile(pMgmt) != 0) {
    dError("failed to read file since %s", terrstr());
    goto _OVER;
  }

  dInfo("bnode start to open");
  bmInitOption(pDnode, &option);
  code = bmOpen(pMgmt, &option);

_OVER:
  if (code == 0) {
    pWrapper->pMgmt = pMgmt;
    dInfo("bnode-mgmt is initialized");
  } else {
    dError("failed to init bnode-mgmt since %s", terrstr());
    bmCleanup(pWrapper);
  }

  return code;
}

static void bmCleanup(SMgmtWrapper *pWrapper) {
  SBnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("bnode-mgmt start to cleanup");
  if (pMgmt->pBnode) {
    bmStopWorker(pMgmt);
    bndClose(pMgmt->pBnode);
    pMgmt->pBnode = NULL;
  }
  free(pMgmt);
  pWrapper->pMgmt = NULL;
  dInfo("bnode-mgmt is cleaned up");
}

void bmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = bmInit;
  mgmtFp.closeFp = bmCleanup;
  mgmtFp.requiredFp = bmRequire;

  bmInitMsgHandles(pWrapper);
  pWrapper->name = "bnode";
  pWrapper->fp = mgmtFp;
}
