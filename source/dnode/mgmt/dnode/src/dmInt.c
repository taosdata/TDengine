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
#include "dmInt.h"

void dmGetMnodeEpSet(SDnodeMgmt *pMgmt, SEpSet *pEpSet) {
  taosRLockLatch(&pMgmt->latch);
  *pEpSet = pMgmt->mnodeEpSet;
  taosRUnLockLatch(&pMgmt->latch);
}

void dmUpdateMnodeEpSet(SDnodeMgmt *pMgmt, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  taosWLockLatch(&pMgmt->latch);
  pMgmt->mnodeEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }

  taosWUnLockLatch(&pMgmt->latch);
}

void dmGetDnodeEp(SMgmtWrapper *pWrapper, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;
  taosRLockLatch(&pMgmt->latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->ep.port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    }
  }

  taosRUnLockLatch(&pMgmt->latch);
}

void dmSendRedirectRsp(SDnodeMgmt *pMgmt, SRpcMsg *pReq) {
  SDnode *pDnode = pMgmt->pDnode;

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pMgmt, &epSet);

  dDebug("RPC %p, req is redirected, num:%d use:%d", pReq->handle, epSet.numOfEps, epSet.inUse);
  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
    if (strcmp(epSet.eps[i].fqdn, pDnode->localFqdn) == 0 && epSet.eps[i].port == pDnode->serverPort) {
      epSet.inUse = (i + 1) % epSet.numOfEps;
    }

    epSet.eps[i].port = htons(epSet.eps[i].port);
  }

  rpcSendRedirectRsp(pReq->handle, &epSet);
}

static int32_t dmStart(SMgmtWrapper *pWrapper) {
  dDebug("dnode-mgmt start to run");
  return dmStartThread(pWrapper->pMgmt);
}

int32_t dmInit(SMgmtWrapper *pWrapper) {
  SDnode     *pDnode = pWrapper->pDnode;
  SDnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SDnodeMgmt));
  dInfo("dnode-mgmt start to init");

  pDnode->dnodeId = 0;
  pDnode->dropped = 0;
  pDnode->clusterId = 0;
  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pDnode;
  pMgmt->pWrapper = pWrapper;
  taosInitRWLatch(&pMgmt->latch);

  pMgmt->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadFile(pMgmt) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pDnode->dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  if (dmStartWorker(pMgmt) != 0) {
    return -1;
  }

  pWrapper->pMgmt = pMgmt;
  dInfo("dnode-mgmt is initialized");
  return 0;
}

void dmCleanup(SMgmtWrapper *pWrapper) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("dnode-mgmt start to clean up");
  dmStopWorker(pMgmt);

  taosWLockLatch(&pMgmt->latch);

  if (pMgmt->dnodeEps != NULL) {
    taosArrayDestroy(pMgmt->dnodeEps);
    pMgmt->dnodeEps = NULL;
  }

  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }

  taosWUnLockLatch(&pMgmt->latch);

  taosMemoryFree(pMgmt);
  pWrapper->pMgmt = NULL;
  dInfo("dnode-mgmt is cleaned up");
}

int32_t dmRequire(SMgmtWrapper *pWrapper, bool *required) {
  *required = true;
  return 0;
}

void dmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInit;
  mgmtFp.closeFp = dmCleanup;
  mgmtFp.startFp = dmStart;
  mgmtFp.requiredFp = dmRequire;

  dmInitMsgHandles(pWrapper);
  pWrapper->name = "dnode";
  pWrapper->fp = mgmtFp;
}
